import {
  BeforeApplicationShutdown,
  Injectable,
  InternalServerErrorException,
  Logger,
  OnApplicationBootstrap,
} from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import { ethers } from "ethers";
import { LRUCache } from "lru-cache";
import { EntityManager } from "typeorm";
import { IPayloadMessage } from "../../../libs/ftso-core/src/fsp-utils/PayloadMessage";
import {
  IProtocolMessageMerkleData,
  IProtocolMessageMerkleRoot,
} from "../../../libs/ftso-core/src/fsp-utils/ProtocolMessageMerkleRoot";
import { DataAvailabilityStatus, DataManager } from "../../../libs/ftso-core/src/DataManager";
import { IndexerClient } from "../../../libs/ftso-core/src/IndexerClient";
import { RewardEpochManager } from "../../../libs/ftso-core/src/RewardEpochManager";
import { ContractMethodNames } from "../../../libs/contracts/src/definitions";
import {
  EPOCH_SETTINGS,
  FTSO2_PROTOCOL_ID,
  RANDOM_GENERATION_BENCHING_WINDOW,
} from "../../../libs/ftso-core/src/constants";
import { calculateResultsForVotingRound } from "../../../libs/ftso-core/src/ftso-calculation/ftso-calculation-logic";
import { CommitData, ICommitData } from "../../../libs/ftso-core/src/data/CommitData";
import { FeedValueEncoder } from "../../../libs/ftso-core/src/data/FeedValueEncoder";
import { FeedResultWithProof, MerkleTreeStructs } from "../../../libs/ftso-core/src/data/MerkleTreeStructs";
import { IRevealData } from "../../../libs/ftso-core/src/data/RevealData";
import { errorString } from "../../../libs/ftso-core/src/utils/error";
import { retry, RetryError } from "../../../libs/ftso-core/src/utils/retry";
import { Bytes32 } from "../../../libs/ftso-core/src/utils/sol-types";
import { EpochResult, Feed, MedianCalculationResult } from "../../../libs/ftso-core/src/voting-types";
import { JSONAbiDefinition } from "./dto/data-provider-responses.dto";
import { Api, FeedId, FeedValuesResponse } from "./feed-value-provider-api/generated/provider-api";

import { EpochResultDiskCache, signingPolicyMismatch } from "./utils/EpochResultDiskCache";
import { RewardEpoch } from "../../../libs/ftso-core/src/RewardEpoch";
import { AbiCache } from "../../../libs/contracts/src/abi/AbiCache";
import { CONTRACTS } from "../../../libs/contracts/src/constants";
import { AxiosResponse } from "axios";

type RoundAndAddress = string;

export interface ResultData {
  message: IProtocolMessageMerkleRoot;
  /** The random number and its Merkle proof, undefined when they could not be produced. */
  finalizationData?: string;
}

/** Outcome of a disk cache rebuild pass, see {@link FtsoDataProviderService.rebuildDiskCache}. */
export interface DiskCacheRebuildSummary {
  fromVotingRoundId: number;
  toVotingRoundId: number;
  /** Rounds whose cached result matched the round's signing policy and was kept. */
  verified: number;
  /** Rounds computed and cached because no cached result existed or it was discarded. */
  rebuilt: number;
  /** Rounds without a complete result to cache; they are computed again on request. */
  unavailable: number;
  /** Rounds whose verification or computation threw. */
  failed: number;
  /** True if the pass stopped early, on shutdown or after repeated failures. */
  interrupted: boolean;
}

/** A rebuild pass gives up after this many consecutive failed rounds (e.g. the indexer is unreachable). */
const MAX_CONSECUTIVE_REBUILD_FAILURES = 10;
const REBUILD_PROGRESS_LOG_INTERVAL = 500;

@Injectable()
export class FtsoDataProviderService implements OnApplicationBootstrap, BeforeApplicationShutdown {
  private readonly logger = new Logger(FtsoDataProviderService.name);

  // connections to the indexer and feed value provider
  private readonly indexerClient: IndexerClient;
  private readonly feedValueProviderClient: Api<unknown>;
  private readonly votingRoundData: LRUCache<RoundAndAddress, IRevealData>;

  /**
   * LRU cache of finalized round results. Recomputing an EpochResult is
   * expensive (queries and parses RANDOM_GENERATION_BENCHING_WINDOW + 1
   * rounds of submit1/submit2 calldata, then medians for every feed), and
   * results are immutable once the reveal deadline passes — so external
   * median/proof queries can be served from memory. Warmed every round by
   * the regular signing flow (getResultData). Opt-in: undefined = caching
   * disabled (EPOCH_RESULT_CACHE_SIZE unset or 0), the v1.1.1-compatible
   * default.
   */
  private readonly epochResultCache: LRUCache<number, EpochResult> | undefined;

  /**
   * Disk-backed second cache level for finalized round results, checked on a
   * memory-cache miss before recomputing. Unlike the LRU above it survives
   * restarts, so a redeploy doesn't expose the provider to a recompute storm
   * from clients backfilling history. An entry is served only while it
   * matches the signing policy resolved for its round, so results written by
   * an older build that resolved a different one are replaced. Opt-in: undefined = disabled
   * (EPOCH_RESULT_DISK_CACHE_SIZE unset or 0, the v1.1.1-compatible default)
   * or the cache directory is unusable.
   */
  private readonly epochResultDiskCache: EpochResultDiskCache | undefined;

  /**
   * In-flight coalescing for result computation: N concurrent requests for
   * the same round share one computation instead of N. Prevents request
   * floods from multiplying indexer load and heap usage (observed cause of
   * an 8GB OOM under repeated external medianCalculationResults queries).
   */
  private readonly epochResultInFlight = new Map<number, Promise<EpochResult | undefined>>();

  /**
   * First voting round of the background disk cache rebuild started on
   * application bootstrap, 0 = no rebuild (EPOCH_RESULT_REBUILD_FROM_ROUND).
   * Used once after a fix that changes past results, so the cache is
   * corrected for that whole range instead of round by round on request.
   */
  private readonly epochResultRebuildFromRound: number;
  private shutdownInitiated = false;

  private readonly rewardEpochManager: RewardEpochManager;
  private readonly dataManager: DataManager;
  private readonly encodingUtils = AbiCache.instance;

  // Indexer top timeout margin
  private readonly indexer_top_timeout: number;

  constructor(manager: EntityManager, configService: ConfigService) {
    const required_history_sec = configService.get<number>("required_indexer_history_time_sec");
    this.indexer_top_timeout = configService.get<number>("indexer_top_timeout");
    this.indexerClient = new IndexerClient(manager, required_history_sec, new Logger(IndexerClient.name));
    this.rewardEpochManager = new RewardEpochManager(this.indexerClient);
    const feedValueProviderTimeoutMs = configService.get<number>("feed_value_provider_timeout_ms") ?? 30_000;
    const feedValueProviderMaxResponseBytes =
      configService.get<number>("feed_value_provider_max_response_bytes") ?? 10 * 1024 * 1024;
    this.feedValueProviderClient = new Api({
      baseURL: configService.get<string>("value_provider_url"),
      timeout: feedValueProviderTimeoutMs,
      maxContentLength: feedValueProviderMaxResponseBytes,
      maxBodyLength: feedValueProviderMaxResponseBytes,
    });
    this.dataManager = new DataManager(this.indexerClient, this.rewardEpochManager, this.logger);
    this.votingRoundData = new LRUCache({
      max: configService.get<number>("voting_round_history_size"),
    });
    const epochResultCacheSize = configService.get<number>("epoch_result_cache_size") ?? 0;
    this.epochResultCache = epochResultCacheSize > 0 ? new LRUCache({ max: epochResultCacheSize }) : undefined;
    const diskCacheSize = configService.get<number>("epoch_result_disk_cache_size") ?? 0;
    if (diskCacheSize > 0) {
      const diskCacheDir = configService.get<string>("epoch_result_disk_cache_dir") ?? "./cache/median/";
      const diskCache = new EpochResultDiskCache(diskCacheDir, diskCacheSize, this.logger);
      try {
        diskCache.init();
        this.epochResultDiskCache = diskCache;
      } catch (e) {
        this.logger.error(`Epoch result disk cache disabled, failed to initialize ${diskCacheDir}: ${errorString(e)}`);
      }
    }
    this.epochResultRebuildFromRound = configService.get<number>("epoch_result_rebuild_from_round") || 0;
  }

  onApplicationBootstrap(): void {
    if (this.epochResultRebuildFromRound <= 0) {
      return;
    }
    if (this.epochResultDiskCache === undefined) {
      this.logger.warn(
        `EPOCH_RESULT_REBUILD_FROM_ROUND ignored: the epoch result disk cache is disabled, results are computed on request`
      );
      return;
    }
    // In the background: neither startup nor the protocol flow waits for it.
    void this.rebuildDiskCache(this.epochResultRebuildFromRound).catch((e) =>
      this.logger.error(`Epoch result disk cache rebuild failed: ${errorString(e)}`)
    );
  }

  beforeApplicationShutdown(): void {
    this.shutdownInitiated = true;
  }

  // Entry point methods for the protocol data provider

  async getCommitData(
    votingRoundId: number,
    submissionAddress: string
  ): Promise<IPayloadMessage<ICommitData> | undefined> {
    const rewardEpoch = await this.rewardEpochManager.getRewardEpochForVotingEpochId(votingRoundId);
    const revealData = await this.calculateOrGetRoundData(votingRoundId, submissionAddress, rewardEpoch);
    const hash = CommitData.hashForCommit(
      submissionAddress,
      votingRoundId,
      revealData.random,
      revealData.encodedValues
    );
    const commitData: ICommitData = {
      commitHash: hash,
    };
    this.logger.log(`Commit for voting round ${votingRoundId}: ${hash}`);
    const msg: IPayloadMessage<ICommitData> = {
      protocolId: FTSO2_PROTOCOL_ID,
      votingRoundId,
      payload: commitData,
    };
    return msg;
  }

  private async calculateOrGetRoundData(votingRoundId: number, submissionAddress: string, rewardEpoch: RewardEpoch) {
    const cached = this.votingRoundData.get(combine(votingRoundId, submissionAddress));
    if (cached !== undefined) {
      this.logger.debug(
        `Returning cached voting round data for ${votingRoundId}: ${submissionAddress} ${cached.random} ${cached.encodedValues}`
      );
      return cached;
    }

    const data = await this.getFeedValuesForEpoch(votingRoundId, rewardEpoch.canonicalFeedOrder);
    this.logger.debug(
      `Got fresh voting round data for ${votingRoundId}: ${submissionAddress} ${data.random} ${data.encodedValues}`
    );
    this.votingRoundData.set(combine(votingRoundId, submissionAddress), data);
    return data;
  }

  getRevealData(votingRoundId: number, submissionAddress: string): IPayloadMessage<IRevealData> | undefined {
    this.logger.log(`Getting reveal for voting round ${votingRoundId}`);

    const revealData = this.votingRoundData.get(combine(votingRoundId, submissionAddress));
    if (revealData === undefined) {
      // we do not have reveal data. Either we committed and restarted the client, hence lost the reveal data irreversibly
      // or we did not commit at all.
      this.logger.error(`No reveal data found for epoch ${votingRoundId}`);
      return undefined;
    }

    const msg: IPayloadMessage<IRevealData> = {
      protocolId: FTSO2_PROTOCOL_ID,
      votingRoundId: votingRoundId,
      payload: revealData,
    };
    return msg;
  }

  /**
   * The protocol message and the trailer Relay v2 requires after the signatures, from a single
   * calculation of the round: this route runs against the submitSignatures deadline and
   * `prepareCalculationResultData` caches nothing.
   */
  async getResultData(votingRoundId: number): Promise<ResultData | undefined> {
    const result = await this.prepareCalculationResultData(votingRoundId);
    if (result === undefined) {
      return undefined;
    }
    const merkleRoot = result.merkleTree.root;
    this.logger.log(`Computed merkle root for voting round ${votingRoundId}: ${merkleRoot}`);
    return {
      message: {
        protocolId: FTSO2_PROTOCOL_ID,
        votingRoundId,
        isSecureRandom: result.randomData.isSecure,
        merkleRoot,
      },
      finalizationData: this.finalizationData(votingRoundId, result),
    };
  }

  /**
   * The random number as a 32-byte word followed by one word per proof node. Never throws: a round without
   * it is still signed and submitted, only this provider's own finalization of it is lost.
   */
  private finalizationData(votingRoundId: number, result: EpochResult): string | undefined {
    try {
      const leaf = MerkleTreeStructs.hashRandomResult(MerkleTreeStructs.fromRandomCalculationResult(result.randomData));
      const proof = result.merkleTree.getProof(leaf);
      if (proof === null) {
        this.logger.warn(`No random merkle proof for voting round ${votingRoundId}`);
        return undefined;
      }
      // the tree hashes the value as a uint256, so it may be stored unpadded; the trailer needs whole words
      const words = [ethers.toBeHex(result.randomData.random, 32), ...proof];
      return "0x" + words.map((word) => word.slice(2)).join("");
    } catch (e) {
      this.logger.warn(`Unable to encode finalization data for voting round ${votingRoundId}: ${errorString(e)}`);
      return undefined;
    }
  }

  async getFullMerkleTree(votingRoundId: number): Promise<IProtocolMessageMerkleData | undefined> {
    const result = await this.prepareCalculationResultData(votingRoundId);
    if (result === undefined) {
      return undefined;
    }
    const merkleRoot = result.merkleTree.root;
    const treeNodes = [
      MerkleTreeStructs.fromRandomCalculationResult(result.randomData),
      ...result.medianData.map((result) => MerkleTreeStructs.fromMedianCalculationResult(result)),
    ];
    const response: IProtocolMessageMerkleData = {
      protocolId: FTSO2_PROTOCOL_ID,
      votingRoundId,
      merkleRoot,
      isSecureRandom: result.randomData.isSecure,
      tree: treeNodes,
    };
    return response;
  }

  async getFeedWithProof(votingRoundId: number, feedId: string): Promise<FeedResultWithProof | undefined> {
    const result = await this.prepareCalculationResultData(votingRoundId);
    if (result === undefined) {
      return undefined;
    }
    const feed = result.medianData.find((median) => median.feed.id === feedId);
    if (feed === undefined) {
      return undefined;
    }
    const proof = result.merkleTree.getProof(MerkleTreeStructs.hashMedianCalculationResult(feed));
    const response: FeedResultWithProof = {
      body: MerkleTreeStructs.fromMedianCalculationResult(feed),
      proof,
    };
    return response;
  }

  async getFullMedianData(votingRoundId: number): Promise<MedianCalculationResult[] | undefined> {
    const result = await this.prepareCalculationResultData(votingRoundId);
    if (result === undefined) {
      return undefined;
    }
    return result.medianData;
  }

  private async prepareCalculationResultData(votingRoundId: number): Promise<EpochResult | undefined> {
    const cached = this.epochResultCache?.get(votingRoundId);
    if (cached !== undefined) {
      this.logger.debug(`Epoch result for round ${votingRoundId} served from memory cache`);
      return cached;
    }

    const inFlight = this.epochResultInFlight.get(votingRoundId);
    if (inFlight !== undefined) {
      return inFlight;
    }

    const computation = this.loadOrComputeCalculationResultData(votingRoundId);
    this.epochResultInFlight.set(votingRoundId, computation);
    try {
      return await computation;
    } finally {
      this.epochResultInFlight.delete(votingRoundId);
    }
  }

  private async loadOrComputeCalculationResultData(votingRoundId: number): Promise<EpochResult | undefined> {
    const fromDisk = await this.loadVerifiedFromDiskCache(votingRoundId);
    if (fromDisk !== undefined) {
      this.logger.log(`Epoch result for round ${votingRoundId} read from disk cache`);
      this.epochResultCache?.set(votingRoundId, fromDisk);
      return fromDisk;
    }
    return this.computeCalculationResultData(votingRoundId);
  }

  /**
   * Reads a round from the disk cache, accepting it only if it was computed against the signing policy resolved
   * for the round now (see {@link signingPolicyMismatch}). The cache outlives the build that wrote it, so a
   * mismatching entry is deleted and the round recomputed. If the reward epoch cannot be resolved, or resolves to
   * a known-stale one (see {@link staleSigningPolicy}), the entry is kept but not served, and the recompute path
   * reports why.
   */
  private async loadVerifiedFromDiskCache(votingRoundId: number): Promise<EpochResult | undefined> {
    const fromDisk = await this.epochResultDiskCache?.get(votingRoundId);
    if (fromDisk === undefined) {
      return undefined;
    }
    let rewardEpoch: RewardEpoch | undefined;
    try {
      rewardEpoch = await this.rewardEpochManager.getRewardEpochForVotingEpochId(votingRoundId);
      const stale = rewardEpoch && (await this.staleSigningPolicy(votingRoundId, rewardEpoch));
      if (stale) {
        this.logger.warn(`Cannot verify cached result for round ${votingRoundId}: ${stale}`);
        return undefined;
      }
    } catch (e) {
      this.logger.warn(`Cannot verify cached result for round ${votingRoundId}: ${errorString(e)}`);
      return undefined;
    }
    if (rewardEpoch === undefined) {
      return undefined;
    }
    const mismatch = signingPolicyMismatch(fromDisk, rewardEpoch);
    if (mismatch !== undefined) {
      this.logger.warn(`Discarding cached result for round ${votingRoundId}: ${mismatch}`);
      await this.epochResultDiskCache.delete(votingRoundId);
      return undefined;
    }
    return fromDisk;
  }

  /**
   * Returns why the reward epoch resolved for a round is known to be stale, or undefined. A round resolves to the
   * latest indexed signing policy starting at or before it, so if the indexer lacks a newer SigningPolicyInitialized
   * (e.g. emitted by a Relay it does not index) the round silently gets the previous reward epoch's voters and
   * weights. That is stale when the round is more than one reward epoch behind the expected one (a switchover is
   * never delayed that long), or when FlareSystemsManager's RewardEpochStarted for the next reward epoch is indexed
   * at or before the round: that epoch has begun, so its signing policy exists on chain but not in the index.
   */
  private async staleSigningPolicy(votingRoundId: number, rewardEpoch: RewardEpoch): Promise<string | undefined> {
    const expectedRewardEpochId = EPOCH_SETTINGS().expectedRewardEpochForVotingEpoch(votingRoundId);
    if (rewardEpoch.rewardEpochId >= expectedRewardEpochId) {
      return undefined;
    }
    const resolved = `round ${votingRoundId} resolves to reward epoch ${rewardEpoch.rewardEpochId}`;
    const missing = "the indexer lacks its SigningPolicyInitialized event";
    if (rewardEpoch.rewardEpochId < expectedRewardEpochId - 1) {
      return `${resolved}, expected ${expectedRewardEpochId}: ${missing}`;
    }
    const nextStarted = (await this.indexerClient.getStartOfRewardEpochEvent(rewardEpoch.rewardEpochId + 1)).data;
    if (nextStarted !== undefined && nextStarted.startVotingRoundId <= votingRoundId) {
      return `${resolved}, but reward epoch ${nextStarted.rewardEpochId} started at round ${nextStarted.startVotingRoundId}: ${missing}`;
    }
    return undefined;
  }

  /**
   * Verifies the disk-cached result of every finalized round from `fromVotingRoundId` to `toVotingRoundId`, newest
   * first and one round at a time, computing any that is missing or was discarded as not matching its round's
   * signing policy. After a pass the cache serves current results for the whole range instead of correcting them
   * round by round on request. Rounds the cache cannot keep, older than its capacity or than every cached round
   * once it is full, end the pass.
   *
   * Throws without touching the cache if the newest round resolves to a known-stale reward epoch (see
   * {@link staleSigningPolicy}): every round would be verified and rebuilt against that stale signing policy.
   */
  async rebuildDiskCache(
    fromVotingRoundId: number,
    toVotingRoundId = EPOCH_SETTINGS().votingEpochForTime(Date.now()) - 2 // the newest round always finalized
  ): Promise<DiskCacheRebuildSummary> {
    const diskCache = this.epochResultDiskCache;
    if (diskCache === undefined) {
      throw new Error("Epoch result disk cache is disabled");
    }
    const rewardEpoch = await this.rewardEpochManager.getRewardEpochForVotingEpochId(toVotingRoundId);
    if (rewardEpoch === undefined) {
      throw new Error(`No reward epoch for round ${toVotingRoundId}`);
    }
    const stale = await this.staleSigningPolicy(toVotingRoundId, rewardEpoch);
    if (stale !== undefined) {
      throw new Error(`Not rebuilding the disk cache, ${stale}`);
    }
    const from = Math.max(fromVotingRoundId, toVotingRoundId - diskCache.capacity + 1);
    if (from > fromVotingRoundId) {
      this.logger.warn(
        `Rounds ${fromVotingRoundId}..${from - 1} exceed the disk cache capacity of ${diskCache.capacity} rounds and are not rebuilt`
      );
    }
    const summary: DiskCacheRebuildSummary = {
      fromVotingRoundId: from,
      toVotingRoundId,
      verified: 0,
      rebuilt: 0,
      unavailable: 0,
      failed: 0,
      interrupted: false,
    };
    const total = Math.max(0, toVotingRoundId - from + 1);
    const counts = () =>
      `${summary.verified} verified, ${summary.rebuilt} rebuilt, ${summary.unavailable} unavailable, ${summary.failed} failed`;
    this.logger.log(`Rebuilding epoch result disk cache for rounds ${from}..${toVotingRoundId} (${total} rounds)`);

    let consecutiveFailures = 0;
    for (let votingRoundId = toVotingRoundId; votingRoundId >= from; votingRoundId--) {
      if (this.shutdownInitiated) {
        summary.interrupted = true;
        break;
      }
      // The protocol flow keeps caching new rounds during a pass, so the cache may fill with rounds newer than the
      // rest of the range: computing those would only evict them again.
      if (diskCache.wouldEvict(votingRoundId)) {
        this.logger.log(`Disk cache rebuild ends at round ${votingRoundId + 1}: the cache is full of newer rounds`);
        summary.fromVotingRoundId = votingRoundId + 1;
        break;
      }
      try {
        if ((await this.loadVerifiedFromDiskCache(votingRoundId)) !== undefined) {
          summary.verified++;
        } else if (
          (await this.prepareCalculationResultData(votingRoundId)) !== undefined &&
          diskCache.has(votingRoundId)
        ) {
          summary.rebuilt++;
        } else {
          summary.unavailable++;
        }
        consecutiveFailures = 0;
      } catch (e) {
        summary.failed++;
        this.logger.warn(`Disk cache rebuild of round ${votingRoundId} failed: ${errorString(e)}`);
        if (++consecutiveFailures >= MAX_CONSECUTIVE_REBUILD_FAILURES) {
          this.logger.error(`Disk cache rebuild stopped after ${consecutiveFailures} consecutive failures`);
          summary.interrupted = true;
          break;
        }
      }
      const done = toVotingRoundId - votingRoundId + 1;
      if (done % REBUILD_PROGRESS_LOG_INTERVAL === 0) {
        this.logger.log(`Disk cache rebuild at round ${votingRoundId}, ${done}/${total}: ${counts()}`);
      }
    }
    this.logger.log(
      `Disk cache rebuild of rounds ${from}..${toVotingRoundId} ${summary.interrupted ? "stopped" : "complete"}: ${counts()}`
    );
    return summary;
  }

  private async computeCalculationResultData(votingRoundId: number): Promise<EpochResult | undefined> {
    const dataResponse = await this.dataManager.getDataForCalculations(
      votingRoundId,
      RANDOM_GENERATION_BENCHING_WINDOW(),
      this.indexer_top_timeout
    );
    if (
      dataResponse.status !== DataAvailabilityStatus.OK &&
      dataResponse.status !== DataAvailabilityStatus.TIMEOUT_OK
    ) {
      this.logger.error(`Data not available for epoch ${votingRoundId}`);
      return undefined;
    }
    // A result against a known-stale signing policy is wrong for every caller, signing included: neither serve
    // nor cache it, so clients retry once the indexer has the missing signing policy.
    const stale = await this.staleSigningPolicy(votingRoundId, dataResponse.data.rewardEpoch);
    if (stale !== undefined) {
      this.logger.error(`Result for round ${votingRoundId} not available, ${stale}`);
      return undefined;
    }
    try {
      const result = calculateResultsForVotingRound(dataResponse.data);
      // Cache only results computed from complete indexer data. A TIMEOUT_OK
      // result may be built from partial data and must stay recomputable.
      if (dataResponse.status === DataAvailabilityStatus.OK) {
        this.epochResultCache?.set(votingRoundId, result);
        await this.epochResultDiskCache?.set(result);
      }
      return result;
    } catch (e) {
      this.logger.error(`Error calculating result: ${errorString(e)}`);
      throw new InternalServerErrorException(`Unable to calculate result for epoch ${votingRoundId}`, { cause: e });
    }
  }

  getAbiDefinitions(): JSONAbiDefinition[] {
    const randomDef = this.encodingUtils.getFunctionInputAbiData(
      CONTRACTS.FtsoMerkleStructs.name,
      ContractMethodNames.randomStruct,
      0
    );
    const feedDef = this.encodingUtils.getFunctionInputAbiData(
      CONTRACTS.FtsoMerkleStructs.name,
      ContractMethodNames.feedStruct,
      0
    );
    const feedWithProof = this.encodingUtils.getFunctionInputAbiData(
      CONTRACTS.FtsoMerkleStructs.name,
      ContractMethodNames.feedWithProofStruct,
      0
    );
    return [
      { abiName: ContractMethodNames.randomStruct, data: randomDef },
      {
        abiName: ContractMethodNames.feedStruct,
        data: feedDef,
      },
      {
        abiName: ContractMethodNames.feedWithProofStruct,
        data: feedWithProof,
      },
    ];
  }

  // Internal methods

  private async getFeedValuesForEpoch(votingRoundId: number, supportedFeeds: Feed[]): Promise<IRevealData> {
    let response: AxiosResponse<FeedValuesResponse, unknown>;

    try {
      response = await retry(
        async () =>
          await this.feedValueProviderClient.feedValueProviderApi.getFeedValues(votingRoundId, {
            feeds: supportedFeeds.map((feed) => decodeFeed(feed.id)),
          })
      );
    } catch (e) {
      if (e instanceof RetryError) {
        throw new Error(
          `Failed to get feed values for epoch ${votingRoundId}, error connecting to value provider:\n${e.cause as Error}`
        );
      }
    }

    if (response.status < 200 || response.status >= 300) {
      throw new Error(`Failed to get feed values for epoch ${votingRoundId}: ${JSON.stringify(response.data)}`);
    }

    const values = response.data;

    // transfer values to 4 byte hex strings and concatenate them
    // make sure that the order of values is in line with protocol definition
    const extractedValues = values.data.map((d) => d.value);

    return {
      values: extractedValues,
      feeds: supportedFeeds,
      random: Bytes32.random().toString(),
      encodedValues: FeedValueEncoder.encode(extractedValues, supportedFeeds),
    };
  }
}

// Helpers

function combine(round: number, address: string): RoundAndAddress {
  return [round, address].toString();
}

function decodeFeed(feedIdHex: string): FeedId {
  feedIdHex = unPrefix0x(feedIdHex);
  if (feedIdHex.length !== 42) {
    throw new Error(`Invalid feed string: ${feedIdHex}`);
  }

  const category = parseInt(feedIdHex.slice(0, 2));
  const name = Buffer.from(feedIdHex.slice(2), "hex").toString("utf8").replaceAll("\0", "");
  return { category, name };
}

function unPrefix0x(tx: string) {
  if (!tx) {
    return "0x0";
  } else if (tx.startsWith("0x") || tx.startsWith("0X")) {
    return tx.slice(2);
  } else if (tx.startsWith("-0x") || tx.startsWith("-0X")) {
    return tx.slice(3);
  }
  return tx;
}

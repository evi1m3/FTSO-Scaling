import { EPOCH_SETTINGS } from "../../../libs/ftso-core/src/constants";

import { CallHandler, Logger } from "@nestjs/common";
import { ConfigService } from "@nestjs/config";
import FakeTimers from "@sinonjs/fake-timers";
import axios from "axios";
import MockAdapter from "axios-mock-adapter";
import { expect } from "chai";
import { firstValueFrom, of } from "rxjs";
import { IConfig } from "../../../apps/ftso-data-provider/src/config/configuration";
import { FtsoDataProviderService } from "../../../apps/ftso-data-provider/src/ftso-data-provider.service";
import {
  encodeCommitPayloadMessage,
  encodeRevealPayloadMessage,
} from "../../../apps/ftso-data-provider/src/response-encoders";
import { ContractMethodNames } from "../../../libs/contracts/src/definitions";
import { MerkleTreeStructs } from "../../../libs/ftso-core/src/data/MerkleTreeStructs";
import { verifyWithMerkleProof } from "../../../libs/ftso-core/src/utils/MerkleTree";
import { CommitData } from "../../../libs/ftso-core/src/data/CommitData";
import { unPrefix0x } from "../../../libs/ftso-core/src/utils/encoding";
import { EpochResult, Feed } from "../../../libs/ftso-core/src/voting-types";
import { TestVoter, generateEvent, generateTx, generateVoters } from "../../utils/basic-generators";
import { MockIndexerDB } from "../../utils/db";
import { currentTimeSec, generateRewardEpochEvents, toFeedId } from "../../utils/generators";
import { getTestFile } from "../../utils/getTestFile";
import { generateRandomAddress } from "../../utils/testRandom";
import { AbiCache } from "../../../libs/contracts/src/abi/AbiCache";
import { CONTRACTS } from "../../../libs/contracts/src/constants";
import { EpochResultDiskCache } from "../../../apps/ftso-data-provider/src/utils/EpochResultDiskCache";
import { BigIntInterceptor } from "../../../apps/ftso-data-provider/src/utils/BigIntInterceptor";
import { RewardEpochStarted } from "../../../libs/contracts/src/events";
import { prepareResultsForVotingRound } from "../../../libs/ftso-core/src/ftso-calculation/ftso-calculation-logic";
import { bigIntReplacer, bigIntReviver } from "../../../libs/ftso-core/src/utils/big-number-serialization";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";

export const testFeeds: Feed[] = [
  { id: toFeedId("BTC/USD", true), decimals: 2 }, // BTC USDT 38,573.26
  { id: toFeedId("ETH/USD", true), decimals: 2 }, // ETH USDT 2,175.12
  { id: toFeedId("FRL/USD", true), decimals: 5 }, // FLR USDT 0.02042
];

describe(`ftso-data-provider.service (${getTestFile(__filename)})`, () => {
  const sampleValues = [38573.26, 2175.12, 0.02042];

  const offerCount = 2;
  const indexerHistorySec = 1000;
  const enc = AbiCache.instance;

  const sigCommit = enc.getFunctionSignature(CONTRACTS.Submission.name, ContractMethodNames.submit1);
  const sigReveal = enc.getFunctionSignature(CONTRACTS.Submission.name, ContractMethodNames.submit2);

  const configValues: IConfig = {
    required_indexer_history_time_sec: indexerHistorySec,
    indexer_top_timeout: 1000,
    voting_round_history_size: 10000,
    value_provider_url: "http://localhost:3000",
    feed_value_provider_timeout_ms: 30000,
    feed_value_provider_max_response_bytes: 10 * 1024 * 1024,
    port: -1,
    db_host: "",
    db_name: "",
    db_user: "",
    db_pass: "",
    db_port: -1,
    api_keys: [],
    // Round ids repeat across tests, so a shared on-disk cache would leak
    // results between them; tests that exercise it use a per-test temp dir.
    epoch_result_disk_cache_size: 0,
    fdc_result_cache_size: 0,
  };

  const configService = new ConfigService(configValues);

  let db: MockIndexerDB;
  let clock: FakeTimers.InstalledClock;
  let mock: MockAdapter;

  before(async () => {
    // Disable NestJS logging
    Logger.overrideLogger(false);
  });

  after(async () => {
    // Re-enable NestJS logging
    Logger.overrideLogger(new Logger());
  });

  beforeEach(async function () {
    db = await MockIndexerDB.create();
    clock = FakeTimers.install({ now: EPOCH_SETTINGS().expectedRewardEpochStartTimeSec(0) * 1000 });
    mock = new MockAdapter(axios);
  });

  afterEach(async () => {
    await db.close();
    clock.uninstall();
    mock.restore();
  });

  it("should configure feed value provider request bounds", async () => {
    const boundedConfigService = new ConfigService({
      ...configValues,
      feed_value_provider_timeout_ms: 1234,
      feed_value_provider_max_response_bytes: 5678,
    });
    const service = new FtsoDataProviderService(db.em, boundedConfigService);
    const client = (service as any).feedValueProviderClient;

    expect(client.instance.defaults.timeout).to.equal(1234);
    expect(client.instance.defaults.maxContentLength).to.equal(5678);
    expect(client.instance.defaults.maxBodyLength).to.equal(5678);
  });

  it("emits reveal data whose hash matches the corresponding commit hash", async () => {
    const voters: TestVoter[] = generateVoters(1);
    const rewardEpochId = 1;
    await setUpRewardEpoch(rewardEpochId, voters);

    mock.onPost(/feed-values/).reply(200, {
      votingRoundId: 1,
      data: testFeeds.map((_, id) => ({ value: sampleValues[id] })),
    });

    const service = new FtsoDataProviderService(db.em, configService);

    const submissionAddress = generateRandomAddress();
    const votingRound = EPOCH_SETTINGS().expectedFirstVotingRoundForRewardEpoch(rewardEpochId);

    const commit = (await service.getCommitData(votingRound, submissionAddress)).payload;

    const reveal = service.getRevealData(votingRound, submissionAddress).payload;

    const expectedCommit = CommitData.hashForCommit(
      submissionAddress,
      votingRound,
      reveal.random,
      reveal.encodedValues
    );
    expect(commit.commitHash).to.be.equal(expectedCommit);
  });

  it("computes identical merkle roots across voters when all reveal the same feed values", async () => {
    const voters: TestVoter[] = generateVoters(10);
    const rewardEpochId = 1;
    await setUpRewardEpoch(rewardEpochId, voters);

    // All voters return the same values at the moment
    mock.onPost(/feed-values/).reply(200, {
      votingRoundId: 1,
      data: testFeeds.map((_, id) => ({ value: sampleValues[id] })),
    });

    const services = voters.map(() => new FtsoDataProviderService(db.em, configService));
    const votingRound = EPOCH_SETTINGS().expectedFirstVotingRoundForRewardEpoch(rewardEpochId);

    clock.tick(1000);

    for (let i = 0; i < voters.length; i++) {
      const encodedCommit = encodeCommitPayloadMessage(
        await services[i].getCommitData(votingRound, voters[i].submitAddress)
      );
      const commitPayload = sigCommit + unPrefix0x(encodedCommit);
      const commitTx = generateTx(
        voters[i].submitAddress,
        CONTRACTS.Submission.address,
        sigCommit,
        1,
        currentTimeSec(),
        commitPayload
      );
      await db.addTransaction([commitTx]);
    }

    clock.tick(EPOCH_SETTINGS().votingEpochDurationSeconds * 1000);

    for (let i = 0; i < voters.length; i++) {
      const encodedReveal = encodeRevealPayloadMessage(services[i].getRevealData(votingRound, voters[i].submitAddress));
      const revealPayload = sigReveal + unPrefix0x(encodedReveal);
      const revealTx = generateTx(
        voters[i].submitAddress,
        CONTRACTS.Submission.address,
        sigReveal,
        2,
        currentTimeSec(),
        revealPayload
      );
      await db.addTransaction([revealTx]);
    }

    clock.tick(EPOCH_SETTINGS().revealDeadlineSeconds * 1000 + 1);

    await db.syncTimeToNow();

    const mRoots = new Set<string>();
    for (let i = 0; i < voters.length; i++) {
      const { message: result, finalizationData } = await services[i].getResultData(votingRound);
      expect(result.votingRoundId).to.be.equal(votingRound);
      expect(result.isSecureRandom).to.be.equal(true);
      mRoots.add(result.merkleRoot);

      const fullMerkleTree = await services[i].getFullMerkleTree(votingRound);
      expect(fullMerkleTree.merkleRoot).to.be.equal(result.merkleRoot);
      expect(fullMerkleTree.isSecureRandom).to.be.equal(true);

      // What Relay v2 does with finalizationData: split it into 32-byte words, rebuild the leaf from the
      // first word and the voting round and secure flag of the signed message, then fold the remaining
      // words to the signed merkle root. Getting any of that wrong reverts the finalization on chain.
      const words = finalizationData.slice(2).match(/.{64}/g);
      expect(finalizationData.length).to.be.equal(2 + 64 * words.length);
      expect(
        verifyWithMerkleProof(
          MerkleTreeStructs.hashRandomResult({
            votingRoundId: result.votingRoundId,
            value: "0x" + words[0],
            isSecure: result.isSecureRandom,
          }),
          words.slice(1).map((word) => "0x" + word),
          result.merkleRoot
        )
      ).to.be.true;
    }
    expect(mRoots.size).to.be.equal(1);
  });

  it("keeps memory-cached results usable after a response serializes them", async () => {
    const voters: TestVoter[] = generateVoters(3);
    const rewardEpochId = 1;
    await setUpRewardEpoch(rewardEpochId, voters);

    const service = new FtsoDataProviderService(
      db.em,
      new ConfigService({ ...configValues, epoch_result_cache_size: 10 })
    );
    const votingRound = EPOCH_SETTINGS().expectedFirstVotingRoundForRewardEpoch(rewardEpochId);
    await runRoundWithAllVoters(service, voters, votingRound);

    // A medianCalculationResults response goes through the global interceptor with the cached objects.
    const medianData = await service.getFullMedianData(votingRound);
    const next: CallHandler = { handle: () => of({ status: "OK", medianData }) };
    await firstValueFrom(new BigIntInterceptor().intercept(undefined as never, next));

    expect(await service.getFullMerkleTree(votingRound)).to.not.be.undefined;
    const feedWithProof = await service.getFeedWithProof(votingRound, medianData[0].feed.id);
    expect(feedWithProof.proof.length).to.be.greaterThan(0);
  });

  describe("benching", () => {
    it("marks random secure when all voters reveal", async () => {
      await runVotingRounds(10, 0, true);
    });
    it("marks random secure when a minority of revealers are benched", async () => {
      await runVotingRounds(10, 3, true);
    });
    it("marks random insecure when fewer than two non-benched revealers remain", async () => {
      await runVotingRounds(5, 4, false);
    });

    async function runVotingRounds(votersCount: number, missedRevealers: number, expectedLastSecureRandom: boolean) {
      const voters: TestVoter[] = generateVoters(votersCount);
      const rewardEpochId = 1;
      await setUpRewardEpoch(rewardEpochId, voters);

      mock.onPost(/feed-values/).reply(200, {
        votingRoundId: 1,
        data: testFeeds.map((_, id) => ({ value: sampleValues[id] })),
      });
      mock.onPost(/feed-values/).reply(200, {
        votingRoundId: 2,
        data: testFeeds.map((_, id) => ({ value: sampleValues[id] })),
      });

      const services = voters.map(() => new FtsoDataProviderService(db.em, configService));
      const votingRound = EPOCH_SETTINGS().expectedFirstVotingRoundForRewardEpoch(rewardEpochId);

      clock.tick(1000);

      for (let i = 0; i < voters.length; i++) {
        const encodedCommit = encodeCommitPayloadMessage(
          await services[i].getCommitData(votingRound, voters[i].submitAddress)
        );
        const commitPayload = sigCommit + unPrefix0x(encodedCommit);
        const commitTx = generateTx(
          voters[i].submitAddress,
          CONTRACTS.Submission.address,
          sigCommit,
          1,
          currentTimeSec(),
          commitPayload
        );
        await db.addTransaction([commitTx]);
      }

      clock.tick(EPOCH_SETTINGS().votingEpochDurationSeconds * 1000);

      for (let i = 0; i < voters.length; i++) {
        const encodedCommit = encodeCommitPayloadMessage(
          await services[i].getCommitData(votingRound + 1, voters[i].submitAddress)
        );
        const commitPayload = sigCommit + unPrefix0x(encodedCommit);
        const commitTx = generateTx(
          voters[i].submitAddress,
          CONTRACTS.Submission.address,
          sigCommit,
          1,
          currentTimeSec(),
          commitPayload
        );
        await db.addTransaction([commitTx]);
      }

      for (let i = 0; i < voters.length; i++) {
        if (i < missedRevealers) continue;

        const encodedReveal = encodeRevealPayloadMessage(
          services[i].getRevealData(votingRound, voters[i].submitAddress)
        );
        const revealPayload = sigReveal + unPrefix0x(encodedReveal);
        const revealTx = generateTx(
          voters[i].submitAddress,
          CONTRACTS.Submission.address,
          sigReveal,
          2,
          currentTimeSec(),
          revealPayload
        );
        await db.addTransaction([revealTx]);
      }

      clock.tick(EPOCH_SETTINGS().revealDeadlineSeconds * 1000 + 1);

      await db.syncTimeToNow();

      const secureRandom = missedRevealers === 0;
      for (let i = 0; i < voters.length; i++) {
        const { message: result } = await services[i].getResultData(votingRound);
        expect(result.isSecureRandom).to.be.equal(secureRandom);
      }

      clock.tick(EPOCH_SETTINGS().votingEpochStartMs(votingRound + 2) - clock.now + 1);

      for (let i = 0; i < voters.length; i++) {
        const encodedReveal = encodeRevealPayloadMessage(
          services[i].getRevealData(votingRound + 1, voters[i].submitAddress)
        );
        const revealPayload = sigReveal + unPrefix0x(encodedReveal);
        const revealTx = generateTx(
          voters[i].submitAddress,
          CONTRACTS.Submission.address,
          sigReveal,
          2,
          currentTimeSec(),
          revealPayload
        );
        await db.addTransaction([revealTx]);
      }

      clock.tick(EPOCH_SETTINGS().revealDeadlineSeconds * 1000 + 1);

      await db.syncTimeToNow();

      for (let i = 0; i < voters.length; i++) {
        const { message: result } = await services[i].getResultData(votingRound + 1);
        expect(result.isSecureRandom).to.be.equal(expectedLastSecureRandom);
      }
    }
  });

  describe("disk cache", () => {
    let cacheDir: string;

    beforeEach(() => {
      cacheDir = fs.mkdtempSync(path.join(os.tmpdir(), "epoch-result-cache-"));
    });

    afterEach(() => {
      fs.rmSync(cacheDir, { recursive: true, force: true });
    });

    function diskCacheConfig(size: number): ConfigService {
      return new ConfigService({
        ...configValues,
        epoch_result_disk_cache_dir: cacheDir,
        epoch_result_disk_cache_size: size,
      });
    }

    /**
     * Rewrites a round's cache entry as a build that resolved another signing policy would have written it: the
     * first voter's median weight differs from the reward epoch's.
     */
    function writeEntryWithForeignWeights(votingRound: number) {
      const file = path.join(cacheDir, `${votingRound}.json`);
      const stored = JSON.parse(fs.readFileSync(file, "utf8"), bigIntReviver);
      for (const median of stored.medianData) {
        median.weights[0] += 1000n;
      }
      fs.writeFileSync(file, JSON.stringify(stored, bigIntReplacer));
    }

    function cachedWeights(votingRound: number): readonly bigint[] {
      const file = path.join(cacheDir, `${votingRound}.json`);
      const stored = JSON.parse(fs.readFileSync(file, "utf8"), bigIntReviver) as EpochResult;
      return stored.medianData[0].weights;
    }

    it("serves results from disk after a restart, without access to round data", async () => {
      const voters: TestVoter[] = generateVoters(3);
      const rewardEpochId = 1;
      await setUpRewardEpoch(rewardEpochId, voters);

      const service = new FtsoDataProviderService(db.em, diskCacheConfig(10));
      const votingRound = EPOCH_SETTINGS().expectedFirstVotingRoundForRewardEpoch(rewardEpochId);
      await runRoundWithAllVoters(service, voters, votingRound);

      const result = await service.getResultData(votingRound);
      expect(result).to.not.be.undefined;
      expect(fs.existsSync(path.join(cacheDir, `${votingRound}.json`))).to.be.true;

      // "Restart": a fresh service on an indexer DB holding only the reward epoch (needed to verify the entry) and
      // no submissions would compute a result with empty medians, so a matching result proves it was restored from
      // disk.
      const epochOnlyDb = await MockIndexerDB.create();
      try {
        await addRewardEpochEvents(epochOnlyDb, rewardEpochId, voters);
        const restarted = new FtsoDataProviderService(epochOnlyDb.em, diskCacheConfig(10));
        const restoredResult = await restarted.getResultData(votingRound);
        expect(restoredResult.message.merkleRoot).to.be.equal(result.message.merkleRoot);
        expect(restoredResult.finalizationData).to.be.equal(result.finalizationData);

        // The rebuilt merkle tree must produce valid proofs.
        const medianData = await restarted.getFullMedianData(votingRound);
        expect(medianData.length).to.be.equal(testFeeds.length);
        const feedWithProof = await restarted.getFeedWithProof(votingRound, medianData[0].feed.id);
        expect(feedWithProof.proof.length).to.be.greaterThan(0);
      } finally {
        await epochOnlyDb.close();
      }
    });

    it("replaces a cached result that was not computed against its round's signing policy", async () => {
      const voters: TestVoter[] = generateVoters(3);
      const rewardEpochId = 1;
      await setUpRewardEpoch(rewardEpochId, voters);

      const service = new FtsoDataProviderService(db.em, diskCacheConfig(10));
      const votingRound = EPOCH_SETTINGS().expectedFirstVotingRoundForRewardEpoch(rewardEpochId);
      await runRoundWithAllVoters(service, voters, votingRound);

      const result = await service.getResultData(votingRound);
      const weights = cachedWeights(votingRound);
      writeEntryWithForeignWeights(votingRound);

      const restarted = new FtsoDataProviderService(db.em, diskCacheConfig(10));
      const medianData = await restarted.getFullMedianData(votingRound);
      expect(medianData[0].weights).to.deep.equal(weights);
      expect((await restarted.getResultData(votingRound)).message.merkleRoot).to.be.equal(result.message.merkleRoot);
      expect(cachedWeights(votingRound)).to.deep.equal(weights);
    });

    it("rebuilds a round range, replacing foreign cache entries and keeping matching ones", async () => {
      const voters: TestVoter[] = generateVoters(3);
      const rewardEpochId = 1;
      await setUpRewardEpoch(rewardEpochId, voters);

      const service = new FtsoDataProviderService(db.em, diskCacheConfig(10));
      const votingRound = EPOCH_SETTINGS().expectedFirstVotingRoundForRewardEpoch(rewardEpochId);
      await runRoundWithAllVoters(service, voters, votingRound);

      await service.getResultData(votingRound);
      const weights = cachedWeights(votingRound);
      writeEntryWithForeignWeights(votingRound);

      const restarted = new FtsoDataProviderService(db.em, diskCacheConfig(10));
      const first = await restarted.rebuildDiskCache(votingRound, votingRound);
      expect(first).to.include({ verified: 0, rebuilt: 1, unavailable: 0, failed: 0, interrupted: false });
      expect(cachedWeights(votingRound)).to.deep.equal(weights);

      const second = await restarted.rebuildDiskCache(votingRound, votingRound);
      expect(second).to.include({ verified: 1, rebuilt: 0, unavailable: 0, failed: 0, interrupted: false });

      // A range reaching further back than the cache holds starts at the oldest round it can keep.
      const small = new FtsoDataProviderService(db.em, diskCacheConfig(1));
      const clamped = await small.rebuildDiskCache(votingRound - 100, votingRound);
      expect(clamped).to.include({ fromVotingRoundId: votingRound, toVotingRoundId: votingRound, verified: 1 });
    });

    it("stops a rebuild on shutdown", async () => {
      const rewardEpochId = 1;
      await setUpRewardEpoch(rewardEpochId, generateVoters(3));
      const votingRound = EPOCH_SETTINGS().expectedFirstVotingRoundForRewardEpoch(rewardEpochId);

      const service = new FtsoDataProviderService(db.em, diskCacheConfig(10));
      service.beforeApplicationShutdown();
      const summary = await service.rebuildDiskCache(votingRound, votingRound);
      expect(summary).to.include({ verified: 0, rebuilt: 0, failed: 0, interrupted: true });
    });

    /** Writes a placeholder cache entry (no feeds) for a round, before the service under test indexes the cache. */
    async function seedEntry(votingRound: number, capacity = 10) {
      const cache = new EpochResultDiskCache(cacheDir, capacity, new Logger("test"));
      cache.init();
      await cache.set(
        prepareResultsForVotingRound(votingRound, [], { votingRoundId: votingRound, random: 1n, isSecure: true })
      );
    }

    async function rebuildError(service: FtsoDataProviderService, votingRound: number): Promise<string | undefined> {
      try {
        await service.rebuildDiskCache(votingRound, votingRound);
      } catch (e) {
        return (e as Error).message;
      }
      return undefined;
    }

    /**
     * Indexes reward epoch 1 but not reward epoch 2's signing policy, as when the Relay signing it is not indexed,
     * optionally with FlareSystemsManager's start of reward epoch 2, then runs the first round of reward epoch 2.
     * The round resolves to reward epoch 1.
     */
    async function runRoundWithoutItsSigningPolicy(
      service: FtsoDataProviderService,
      voters: TestVoter[],
      withNextEpochStarted: boolean
    ): Promise<number> {
      await setUpRewardEpoch(1, voters);
      const startSec = EPOCH_SETTINGS().expectedRewardEpochStartTimeSec(2);
      const votingRound = EPOCH_SETTINGS().expectedFirstVotingRoundForRewardEpoch(2);
      if (withNextEpochStarted) {
        const started = new RewardEpochStarted({
          rewardEpochId: 2,
          startVotingRoundId: votingRound,
          timestamp: startSec,
        });
        await db.addEvent([
          generateEvent(CONTRACTS.FlareSystemsManager, RewardEpochStarted.eventName, started, 1, startSec),
        ]);
      }
      clock.setSystemTime(startSec * 1000);
      await db.syncTimeToNow();
      await runRoundWithAllVoters(service, voters, votingRound);
      return votingRound;
    }

    it("neither serves nor caches a round whose reward epoch started without an indexed signing policy", async () => {
      const voting = new FtsoDataProviderService(db.em, configService);
      const votingRound = await runRoundWithoutItsSigningPolicy(voting, generateVoters(3), true);
      // An entry cached by another build cannot be verified against a stale signing policy, nor shown wrong by it.
      await seedEntry(votingRound);

      const service = new FtsoDataProviderService(db.em, diskCacheConfig(10));
      expect(await service.getResultData(votingRound)).to.be.undefined;
      expect(await service.getFullMedianData(votingRound)).to.be.undefined;
      expect(await rebuildError(service, votingRound)).to.contain(
        `round ${votingRound} resolves to reward epoch 1, but reward epoch 2 started at round ${votingRound}`
      );
      expect(fs.readdirSync(cacheDir)).to.deep.equal([`${votingRound}.json`]);
    });

    it("serves and caches a round one reward epoch behind while the next one has not started", async () => {
      // A delayed switchover: the round legitimately belongs to the previous reward epoch.
      const service = new FtsoDataProviderService(db.em, diskCacheConfig(10));
      const votingRound = await runRoundWithoutItsSigningPolicy(service, generateVoters(3), false);
      expect(await service.getResultData(votingRound)).to.not.be.undefined;
      expect(fs.existsSync(path.join(cacheDir, `${votingRound}.json`))).to.be.true;
    });

    it("refuses to rebuild a round more than one reward epoch behind", async () => {
      // Only reward epoch 1 is indexed: a round of reward epoch 3 falls back to epoch 1's signing policy.
      await setUpRewardEpoch(1, generateVoters(3));
      const votingRound = EPOCH_SETTINGS().expectedFirstVotingRoundForRewardEpoch(3);
      clock.setSystemTime(EPOCH_SETTINGS().votingEpochStartSec(votingRound + 2) * 1000);
      await db.syncTimeToNow();
      await seedEntry(votingRound);

      const service = new FtsoDataProviderService(db.em, diskCacheConfig(10));
      expect(await rebuildError(service, votingRound)).to.contain(
        `round ${votingRound} resolves to reward epoch 1, expected 3`
      );
      expect(fs.readdirSync(cacheDir)).to.deep.equal([`${votingRound}.json`]);
    });

    it("ends a rebuild at rounds the full cache would evict right away", async () => {
      const rewardEpochId = 1;
      await setUpRewardEpoch(rewardEpochId, generateVoters(3));
      const votingRound = EPOCH_SETTINGS().expectedFirstVotingRoundForRewardEpoch(rewardEpochId);
      // Cached by the protocol flow during the pass: the cache is full of rounds newer than the rest of the range.
      await seedEntry(votingRound + 1, 2);
      await seedEntry(votingRound + 2, 2);

      const service = new FtsoDataProviderService(db.em, diskCacheConfig(2));
      const summary = await service.rebuildDiskCache(votingRound - 1, votingRound);
      expect(summary).to.include({ fromVotingRoundId: votingRound + 1, rebuilt: 0, unavailable: 0, failed: 0 });
      expect(fs.readdirSync(cacheDir).sort()).to.deep.equal([`${votingRound + 1}.json`, `${votingRound + 2}.json`]);
    });

    it("removes temp files left by an interrupted write on init", () => {
      fs.writeFileSync(path.join(cacheDir, "5.json.tmp"), "{");
      new EpochResultDiskCache(cacheDir, 10, new Logger("test")).init();
      expect(fs.readdirSync(cacheDir)).to.be.empty;
    });

    it("evicts the oldest rounds beyond capacity", async () => {
      const cache = new EpochResultDiskCache(cacheDir, 2, new Logger("test"));
      cache.init();
      for (const id of [1, 2, 3]) {
        await cache.set(
          prepareResultsForVotingRound(id, [], { votingRoundId: id, random: BigInt(id), isSecure: true })
        );
      }
      expect(fs.readdirSync(cacheDir).sort()).to.deep.equal(["2.json", "3.json"]);
      expect(await cache.get(1)).to.be.undefined;
      expect((await cache.get(2)).randomData.random).to.be.equal(2n);

      // A new instance over the same directory trims to its own capacity on init.
      const smaller = new EpochResultDiskCache(cacheDir, 1, new Logger("test"));
      smaller.init();
      expect(fs.readdirSync(cacheDir)).to.deep.equal(["3.json"]);
    });

    it("discards unreadable cache entries and falls back to recompute path", async () => {
      const cache = new EpochResultDiskCache(cacheDir, 10, new Logger("test"));
      cache.init();
      await cache.set(prepareResultsForVotingRound(7, [], { votingRoundId: 7, random: 7n, isSecure: true }));
      fs.writeFileSync(path.join(cacheDir, "7.json"), "not json");

      expect(await cache.get(7)).to.be.undefined;
      expect(fs.existsSync(path.join(cacheDir, "7.json"))).to.be.false;
    });
  });

  async function setUpRewardEpoch(rewardEpochId: number, voters: TestVoter[]) {
    const epochEvents = generateRewardEpochEvents(EPOCH_SETTINGS(), testFeeds, offerCount, rewardEpochId, voters);

    await db.addEvent(epochEvents);

    clock.setSystemTime(EPOCH_SETTINGS().expectedRewardEpochStartTimeSec(rewardEpochId) * 1000 + 1);

    await db.syncTimeToNow();
  }

  /** Adds the events defining a reward epoch to another indexer DB, without moving the clock. */
  async function addRewardEpochEvents(target: MockIndexerDB, rewardEpochId: number, voters: TestVoter[]) {
    await target.addEvent(generateRewardEpochEvents(EPOCH_SETTINGS(), testFeeds, offerCount, rewardEpochId, voters));
    await target.syncTimeToNow();
  }

  /** Runs a full commit/reveal round in which every voter submits, leaving the clock past its reveal deadline. */
  async function runRoundWithAllVoters(service: FtsoDataProviderService, voters: TestVoter[], votingRound: number) {
    mock.onPost(/feed-values/).reply(200, {
      votingRoundId: 1,
      data: testFeeds.map((_, id) => ({ value: sampleValues[id] })),
    });

    clock.tick(1000);

    for (const voter of voters) {
      const encodedCommit = encodeCommitPayloadMessage(await service.getCommitData(votingRound, voter.submitAddress));
      const commitPayload = sigCommit + unPrefix0x(encodedCommit);
      await db.addTransaction([
        generateTx(voter.submitAddress, CONTRACTS.Submission.address, sigCommit, 1, currentTimeSec(), commitPayload),
      ]);
    }

    clock.tick(EPOCH_SETTINGS().votingEpochDurationSeconds * 1000);

    for (const voter of voters) {
      const encodedReveal = encodeRevealPayloadMessage(service.getRevealData(votingRound, voter.submitAddress));
      const revealPayload = sigReveal + unPrefix0x(encodedReveal);
      await db.addTransaction([
        generateTx(voter.submitAddress, CONTRACTS.Submission.address, sigReveal, 2, currentTimeSec(), revealPayload),
      ]);
    }

    clock.tick(EPOCH_SETTINGS().revealDeadlineSeconds * 1000 + 1);

    await db.syncTimeToNow();
  }
});

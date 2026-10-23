/* eslint-disable */
import { CallHandler, ExecutionContext, Injectable, NestInterceptor } from "@nestjs/common";
import { Observable, map } from "rxjs";

@Injectable()
export class BigIntInterceptor implements NestInterceptor {
  intercept(context: ExecutionContext, next: CallHandler): Observable<any> {
    return next.handle().pipe(map((data) => this.convertBigIntToString(data)));
  }

  // Builds new arrays and objects rather than assigning into the response: handlers may return objects they keep,
  // such as cached round results, whose bigint fields later requests still compute with.
  private convertBigIntToString(data: any): any {
    if (typeof data === "bigint") {
      return data.toString();
    }
    // typeof null === "object", but Object.keys(null) throws
    if (data === null || typeof data !== "object") {
      return data;
    }

    if (Array.isArray(data)) {
      return data.map((item) => this.convertBigIntToString(item));
    }

    // serialized by its own toJSON (e.g. Date), not by its fields
    if (typeof data.toJSON === "function") {
      return data;
    }

    const converted: Record<string, any> = {};
    for (const key of Object.keys(data)) {
      converted[key] = this.convertBigIntToString(data[key]);
    }
    return converted;
  }
}

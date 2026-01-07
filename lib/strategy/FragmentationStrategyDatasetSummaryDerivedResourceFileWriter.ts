import { writeFile } from 'node:fs/promises';
import { dirname } from 'node:path';
import { mkdirp } from 'mkdirp';

import type { IDatasetSummaryDerivedResource } from '../summary/DatasetSummaryDerivedResource';
import type {
  IFragmentationStrategyDatasetSummaryDerivedResourceOptions,
} from './FragmentationStrategyDatasetSummaryDerivedResource';
import {
  FragmentationStrategyDatasetSummaryDerivedResource,
} from './FragmentationStrategyDatasetSummaryDerivedResource';

export abstract class FragmentationStrategyDatasetSummaryDerivedResourceFileWriter<
  T extends IDatasetSummaryDerivedResource,
> extends FragmentationStrategyDatasetSummaryDerivedResource<T> {
  protected readonly iriToPath: Map<RegExp, string>;

  public constructor(options: IFragmentationStrategyDatasetSummaryDerivedResourceFileWriterOptions) {
    super(options);
    this.iriToPath = new Map(Object.entries(options.iriToPath).map(([ exp, sub ]) => [
      new RegExp(exp, 'u'),
      sub,
    ]));
  }

  protected getFilePath(iri: string): string {
    // Remove hash fragment
    const posHash = iri.indexOf('#');
    if (posHash >= 0) {
      iri = iri.slice(0, posHash);
    }

    // Find base path from the first matching baseIRI
    let bestMatch: RegExpExecArray | undefined;
    let bestRegex: RegExp | undefined;

    for (const exp of this.iriToPath.keys()) {
      const match = exp.exec(iri);
      if (match && (bestMatch === undefined || match[0].length > bestMatch[0].length)) {
        bestMatch = match;
        bestRegex = exp;
      }
    }

    // Crash if we did not find a matching baseIRI
    if (!bestRegex) {
      throw new Error(`No IRI mapping found for ${iri}`);
    }

    // Perform substitution and replace illegal directory names
    let path = iri.replace(bestRegex, this.iriToPath.get(bestRegex)!);

    // Replace illegal directory names
    path = path.replaceAll(/[*|"<>?:]/ug, '_');
    return path;
  }

  protected async writeDirAndFile(path: string, data: string, encoding: BufferEncoding): Promise<void> {
    await mkdirp(dirname(path));
    await writeFile(path, data, encoding);
  }
}

export interface IFragmentationStrategyDatasetSummaryDerivedResourceFileWriterOptions
  extends IFragmentationStrategyDatasetSummaryDerivedResourceOptions {
  /**
   * Mapping of regular expressions to their replacements,
   * for determining the file path from a given IRI.
   * @range {json}
   */
  iriToPath: Record<string, string>;
}

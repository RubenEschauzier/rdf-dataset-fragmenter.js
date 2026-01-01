import { writeFile } from 'node:fs/promises';
import { dirname } from 'node:path';
import { mkdirp } from 'mkdirp';

import type { IQuadSink } from '../io/IQuadSink';
import { DatasetSummaryDerivedResourceQpf } from '../summary/DatasetSummaryDerivedResourceQpf';
import {
  FragmentationStrategyDatasetSummaryDerivedResource,
  type IFragmentationStrategyDatasetSummaryDerivedResourceOptions,
} from './FragmentationStrategyDatasetSummaryDerivedResource';

export class FragmentationStrategyDatasetSummaryDerivedResourceQpf
  extends FragmentationStrategyDatasetSummaryDerivedResource<DatasetSummaryDerivedResourceQpf> {
  protected readonly iriToPath: Map<RegExp, string>;

  public constructor(options: IFragmentationStrategyDatasetSummaryDerivedResourceQpfOptions) {
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

  protected createSummary(dataset: string): DatasetSummaryDerivedResourceQpf {
    return new DatasetSummaryDerivedResourceQpf(
      {
        dataset,
      },
    );
  }

  /**
   * Instead of writing quads, we simply write a filter text file with qpf in it
   * see (https://github.com/SolidLabResearch/derived-resources-component/blob/main/documentation/filters.md)
   * and we generate the required metadata quads for the .meta file
   * @param quadSink unused
   */
  protected override async flush(quadSink: IQuadSink): Promise<void> {
    for (const [ key, summary ] of this.summaries) {
      const { iri } = summary.serialize();
      const filePathPod = this.getFilePath(iri);
      const path = `${filePathPod}${this.filterFilename.replace(':COUNT:', '0')}.txt`;
      await mkdirp(dirname(path));
      await writeFile(path, 'qpf', 'utf-8');

      // Generate and write metadata quads that point towards the qpf resource
      const metadataQuads = this.metadataQuadsGenerator.generateMetadata({
        podUri: iri,
        selectorPattern: `${iri}*`,
        filterFilenameTemplate: this.filterFilename,
        nResources: 1,
      });

      const metaFile = `${iri}${this.metadataQuadsGenerator.getMetaFileName()}`;
      for (const quad of metadataQuads) {
        await quadSink.push(metaFile, quad);
      }
      this.summaries.delete(key);
    }
  }
}

export interface IFragmentationStrategyDatasetSummaryDerivedResourceQpfOptions
  extends IFragmentationStrategyDatasetSummaryDerivedResourceOptions {
  /**
   * Mapping of regular expressions to their replacements,
   * for determining the file path from a given IRI.
   * @range {json}
   */
  iriToPath: Record<string, string>;

}

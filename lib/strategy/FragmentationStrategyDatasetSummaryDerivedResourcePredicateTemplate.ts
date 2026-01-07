import { DataFactory } from 'rdf-data-factory'; // Assumed standard factory
import type { IQuadSink } from '../io/IQuadSink';
import type {
  IFragmentationStrategyDatasetSummaryDerivedResourceFileWriterOptions,
} from './FragmentationStrategyDatasetSummaryDerivedResourceFileWriter';
import {
  FragmentationStrategyDatasetSummaryDerivedResourceFileWriter,
} from './FragmentationStrategyDatasetSummaryDerivedResourceFileWriter';
import { DatasetSummaryDerivedResourceStub } from '../summary/DatasetSummaryDerivedResourceStub';

const DF = new DataFactory();

export class FragmentationStrategyDatasetSummaryDerivedResourcePredicateTemplate
  extends FragmentationStrategyDatasetSummaryDerivedResourceFileWriter<DatasetSummaryDerivedResourceStub> {
  
  protected readonly maxSizeStars: number;

  public constructor(options: IFragmentationStrategyDatasetSummaryDerivedResourcePredictateTemplateOptions) {
    super(options);
    this.maxSizeStars = options.maxSizeStars;
  }

  protected createSummary(dataset: string): DatasetSummaryDerivedResourceStub {
    return new DatasetSummaryDerivedResourceStub({
      dataset,
    });
  }


  protected override async flush(quadSink: IQuadSink): Promise<void> {
    this.processBlankNodes();
    for (const [ key, summary ] of this.summaries) {
        const output = summary.serialize();
        for (let i = 1; i < this.maxSizeStars; i++){
            const queryPatterns: string[] = [];
            for (let j = 1; j <= i; j++) {
                const varName = `$p${j}$`;
                queryPatterns.push(`  ?s ${varName} ?o${j} .`);
            }

            const constructQuery = 
`CONSTRUCT {
${queryPatterns.join('\n')}
}
WHERE {
${queryPatterns.join('\n')}
}`;
            const filePathPod = this.getFilePath(output.iri);
            const path = `${filePathPod}${this.filterFilename.replace(':COUNT:', `${i}`)}.rq`;
            await this.writeDirAndFile(path, constructQuery, 'utf-8');

        }
        const metaFile = `${output.iri}${this.metadataQuadsGenerator.getMetaFileName()}`;
        await this.writeMetaFile(output.iri, this.maxSizeStars-1, quadSink, metaFile);

        if (this.directMetadataLinkPredicate) {
            await this.writeDirectMetadataLink(output, quadSink, metaFile);
        }
        this.summaries.delete(key);
    }
    await super.flush(quadSink);
  }
}

export interface IFragmentationStrategyDatasetSummaryDerivedResourcePredictateTemplateOptions
  extends IFragmentationStrategyDatasetSummaryDerivedResourceFileWriterOptions {
  /**
   * The maximum number of predicates in the star-join ladder.
   * e.g., if set to 3, it generates resources for 1, 2, and 3 predicates.
   */
  maxSizeStars: number;
}
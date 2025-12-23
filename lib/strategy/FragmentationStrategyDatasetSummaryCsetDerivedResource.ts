import type { IQuadSink } from '../io/IQuadSink';
import { DatasetSummaryCsetDerivedResource } from '../summary/DatasetSummaryCsetDerivedResource';
import { FragmentationStrategyDatasetSummary } from './FragmentationStrategyDatasetSummary';
import type { IFragmentationStrategyDatasetSummaryOptions } from './FragmentationStrategyDatasetSummary';

export class FragmentationStrategyDatasetSummaryCsetDerivedResource
  extends FragmentationStrategyDatasetSummary<DatasetSummaryCsetDerivedResource> {
  public derivedResourceStrategy: 'minSize' | 'maxCardinality';
  public maxResources: number;

  public constructor(options: IFragmentationStrategyDatasetSummaryCsetDerivedResourceOptions) {
    super(options);
    this.derivedResourceStrategy = options.derivedResourceStrategy;
    this.maxResources = options.maxResources;
  }

  protected createSummary(dataset: string): DatasetSummaryCsetDerivedResource {
    return new DatasetSummaryCsetDerivedResource(
      {
        dataset,
        derivedResourceConstructionStrategy: this.derivedResourceStrategy,
        maxResources: this.maxResources,
      },
    );
  }

  protected override async flush(quadSink: IQuadSink): Promise<void> {
    this.processBlankNodes();
    for (const [ key, summary ] of this.summaries) {
      const output = summary.serialize();
      let startIdx = 0;
      let iriIdx = 0;
      for (const groupSize of output.grouped) {
        const quadsSingleResource = output.quads.slice(startIdx, startIdx + groupSize);
        for (const quad of quadsSingleResource) {
          await quadSink.push(`${output.iri}-${iriIdx}`, quad);
        }
        startIdx += groupSize;
        iriIdx++;
      }
      this.summaries.delete(key);
    }
    await super.flush(quadSink);
  }
}

export interface IFragmentationStrategyDatasetSummaryCsetDerivedResourceOptions
  extends IFragmentationStrategyDatasetSummaryOptions {
  derivedResourceStrategy: 'minSize' | 'maxCardinality';
  maxResources: number;
}

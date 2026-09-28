import type { Quad } from '@rdfjs/types';
import type {
  IConstructQueryOutput,
  IFragmentationStrategyDatasetSummaryDerivedResourceOptions,
} from './FragmentationStrategyDatasetSummaryDerivedResource';
import { FragmentationStrategyDatasetSummaryDerivedResourceQpf } from './FragmentationStrategyDatasetSummaryDerivedResourceQpf';

/**
 * Generates a pattern batch derived resource for every pod: one that answers several independent
 * quad patterns, passed in its query string, in a single request.
 * Like QPF, its filter is a fixed string, `patterns`, as the patterns come with each request.
 */
export class FragmentationStrategyDatasetSummaryDerivedResourcePatternBatch
  extends FragmentationStrategyDatasetSummaryDerivedResourceQpf {
  public constructor(options: IFragmentationStrategyDatasetSummaryDerivedResourceOptions) {
    super(options);
  }

  /**
   * @param quads Unused
   * @param context Unused
   * @returns "patterns"
   */
  protected override constructQuery(quads: Quad[], context: Record<string, any>): IConstructQueryOutput {
    return { query: 'patterns' };
  }
}

import type * as RDF from '@rdfjs/types';
import type { IDatasetSummaryArgs } from './DatasetSummary';
import { DatasetSummaryDerivedResource, type IDatasetSummarySparqlOutput } from './DatasetSummaryDerivedResource';

/**
 * Stub dataset summary class that does no actual summary creation.
 */
export class DatasetSummaryDerivedResourceStub extends DatasetSummaryDerivedResource {
  public constructor(args: IDatasetSummaryArgs) {
    super(args);
  }

  // eslint-disable-next-line unused-imports/no-unused-vars
  public register(quad: RDF.Quad): void {};

  public serialize(): IDatasetSummarySparqlOutput {
    return { quads: [], iri: this.dataset, grouped: [ 0 ]};
  }
}

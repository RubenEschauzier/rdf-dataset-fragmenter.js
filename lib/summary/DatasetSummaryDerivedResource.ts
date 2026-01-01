import type * as RDF from '@rdfjs/types';
import { DF, type IDatasetSummaryArgs, type IDatasetSummaryOutput } from './DatasetSummary';

export interface IDatasetSummaryDerivedResource {
  register: (quad: RDF.Quad) => void;
  serialize: () => IDatasetSummarySparqlOutput;
}

export abstract class DatasetSummaryDerivedResource implements IDatasetSummaryDerivedResource {
  protected readonly dataset: string;

  /* eslint-disable ts/naming-convention */
  public static readonly RDF_TYPE = DF.namedNode('http://www.w3.org/1999/02/22-rdf-syntax-ns#type');
  public static readonly XSD_INTEGER = DF.namedNode('http://www.w3.org/2001/XMLSchema#integer');
  public static readonly XSD_BASE64 = DF.namedNode('http://www.w3.org/2001/XMLSchema#base64Binary');
  /* eslint-enable ts/naming-convention */

  public constructor(args: IDatasetSummaryArgs) {
    this.dataset = args.dataset;
  }

  public abstract register(quad: RDF.Quad): void;
  public abstract serialize(): IDatasetSummarySparqlOutput;
}

export interface IDatasetSummarySparqlOutput extends IDatasetSummaryOutput {
  /**
   * Indexes indicating which quads belong together, as frag strategies expect
   * flat arrays of quads
   */
  grouped: number[];
}

import type * as RDF from '@rdfjs/types';
import { DataFactory } from 'rdf-data-factory';
import type { IDatasetSummaryArgs, IDatasetSummaryOutput } from './DatasetSummary';
import { DatasetSummary } from './DatasetSummary';

export class DatasetSummaryCsetDerivedResource extends DatasetSummary {
  private readonly subjectMap: Map<string, Set<string>>;
  private readonly constructionStrategy: 'maxCardinality' | 'minSize';
  private readonly maxResources: number;
  private readonly variableReplacementIndicator: string;


  // eslint-disable-next-line ts/naming-convention
  private readonly DF = new DataFactory();

  public constructor(args: IDatasetSummaryCsetDerivedResourceArgs) {
    super(args);
    this.subjectMap = new Map();
    this.constructionStrategy = args.derivedResourceConstructionStrategy;
    this.maxResources = args.maxResources;
    this.variableReplacementIndicator = args.variableReplacementIndicator;
  }

  public register(quad: RDF.Quad): void {
    const subject = quad.subject.value;
    const predicate = quad.predicate.value;

    if (!this.subjectMap.has(subject)) {
      this.subjectMap.set(subject, new Set());
    }

    this.subjectMap.get(subject)!.add(predicate);
  }

  public serialize(): IDatasetSummarySparqlOutput {
    const cSets = new Map<string, ICharacteristicSet>();

    for (const predicates of this.subjectMap.values()) {
      // Create a unique signature for this set of predicates (sort to ensure consistency)
      const predicateArray = [ ...predicates ].sort();
      const signature = JSON.stringify(predicateArray);

      if (!cSets.has(signature)) {
        cSets.set(signature, {
          predicates,
          count: 0,
        });
      }

      // Increment cardinality for this specific shape
      cSets.get(signature)!.count++;
    }
    let selected: ICharacteristicSet[];
    if (this.constructionStrategy === 'maxCardinality') {
      selected = this.selectorMaxCardinality(cSets);
    } else if (this.constructionStrategy === 'minSize') {
      selected = this.selectorMinSize(cSets);
    } else {
      throw new Error(`Passed unsupported derived resource construction strategy in ${this.constructor.name}`);
    }
    // Convert into quads that will form the BGP of the query
    const selectedQuads: RDF.Quad[][] = selected.map(
      (cset) => {
        let objCnt = 0;
        return [ ...cset.predicates ].map(
          pred => this.DF.quad(
            this.DF.namedNode(`${this.variableReplacementIndicator}s0`),
            this.DF.namedNode(pred),
            this.DF.namedNode(`${this.variableReplacementIndicator}o${objCnt++}`),
          ),
        );
      },
    );
    const grouped = selectedQuads.map(quads => quads.length);
    return { quads: selectedQuads.flat(1), iri: this.dataset, grouped };
  }

  private selectorMaxCardinality(cSets: Map<string, ICharacteristicSet>): ICharacteristicSet[] {
    // Sorted high to low
    const sorted: ICharacteristicSet[] = [ ...cSets.values() ].sort((a, b) => b.count - a.count);
    return sorted.slice(0, this.maxResources);
  }

  private selectorMinSize(cSets: Map<string, ICharacteristicSet>): ICharacteristicSet[] {
    const sorted: ICharacteristicSet[] =
      [ ...cSets.values() ].sort((a, b) => a.predicates.size - b.predicates.size);
    return sorted.slice(0, this.maxResources);
  }
}

export interface ICharacteristicSet {
  predicates: Set<string>;
  count: number;
}

export interface IDatasetSummarySparqlOutput extends IDatasetSummaryOutput {
  /**
   * Indexes indicating which quads belong together, as frag strategies expect
   * flat arrays of quads
   */
  grouped: number[];
}

export interface IDatasetSummaryCsetDerivedResourceArgs extends IDatasetSummaryArgs {
  derivedResourceConstructionStrategy: 'minSize' | 'maxCardinality';
  maxResources: number;
  variableReplacementIndicator: string;
}

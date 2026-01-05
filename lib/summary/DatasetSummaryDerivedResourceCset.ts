import type * as RDF from '@rdfjs/types';
import { DataFactory } from 'rdf-data-factory';
import type { IQuadMatcher } from '../quadmatcher/IQuadMatcher';
import type { IDatasetSummaryArgs } from './DatasetSummary';
import { DatasetSummaryDerivedResource, type IDatasetSummarySparqlOutput } from './DatasetSummaryDerivedResource';

export class DatasetSummaryDerivedResourceCset extends DatasetSummaryDerivedResource {
  private readonly subjectMap: Map<string, Set<string>>;
  private readonly filter: IQuadMatcher | undefined;
  private readonly constructionStrategy: | 'minSize' | 'maxSize' | 'maxCardinality';
  private readonly authoritativenessStrategy: 'baseUri' | undefined;
  private readonly maxResources: number;
  private readonly variableReplacementIndicator: string;

  // eslint-disable-next-line ts/naming-convention
  private readonly DF = new DataFactory();

  public constructor(args: IDatasetSummaryDerivedResourceCsetArgs) {
    super(args);
    this.subjectMap = new Map();

    this.constructionStrategy = args.derivedResourceConstructionStrategy;
    this.authoritativenessStrategy = args.authoritativenessStrategy;

    this.maxResources = args.maxResources;
    this.variableReplacementIndicator = args.variableReplacementIndicator;

    this.filter = args.filter;
  }

  public register(quad: RDF.Quad): void {
    if (!this.filter || this.filter.matches(quad)) {
      if (this.authoritativenessStrategy === 'baseUri' &&
        !quad.subject.value.startsWith(this.dataset)) {
        return;
      }
      const subject = quad.subject.value;
      const predicate = quad.predicate.value;

      if (!this.subjectMap.has(subject)) {
        this.subjectMap.set(subject, new Set());
      }

      this.subjectMap.get(subject)!.add(predicate);
    }
  }

  public serialize(): IDatasetSummarySparqlOutput {
    const cSets = new Map<string, ICharacteristicSet>();

    for (const predicates of this.subjectMap.values()) {
      const predicateArray = [ ...predicates ].sort();
      const signature = JSON.stringify(predicateArray);

      if (!cSets.has(signature)) {
        cSets.set(signature, {
          predicates,
          count: 0,
        });
      }

      cSets.get(signature)!.count++;
    }
    let selected: ICharacteristicSet[];
    if (this.constructionStrategy === 'maxCardinality') {
      selected = this.selectorMaxCardinality(cSets);
    } else if (this.constructionStrategy === 'minSize') {
      selected = this.selectorMinSize(cSets);
    } else if (this.constructionStrategy === 'maxSize') {
      selected = this.selectorMaxSize(cSets);
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
    const sorted: ICharacteristicSet[] = [ ...cSets.values() ].sort((a, b) => b.count - a.count);
    return this.filterMinSizeCset(sorted).slice(0, this.maxResources);
  }

  private selectorMinSize(cSets: Map<string, ICharacteristicSet>): ICharacteristicSet[] {
    const sorted: ICharacteristicSet[] =
      [ ...cSets.values() ].sort((a, b) => a.predicates.size - b.predicates.size);
    return this.filterMinSizeCset(sorted).slice(0, this.maxResources);
  }

  private selectorMaxSize(cSets: Map<string, ICharacteristicSet>): ICharacteristicSet[] {
    const sorted: ICharacteristicSet[] =
      [ ...cSets.values() ].sort((a, b) => b.predicates.size - a.predicates.size);
    return this.filterMinSizeCset(sorted).slice(0, this.maxResources);
  }


  private filterMinSizeCset(cSets: ICharacteristicSet[]): ICharacteristicSet[] {
    return cSets.filter(cSet => cSet.predicates.size > 1);
  }
}

export interface ICharacteristicSet {
  predicates: Set<string>;
  count: number;
}

export interface IDatasetSummaryDerivedResourceCsetArgs extends IDatasetSummaryArgs {
  /**
   * How the derived resource triple patterns should be selected from the candidate csets
   */
  derivedResourceConstructionStrategy: 'minSize' | 'maxSize' | 'maxCardinality';
  /**
   * Maximal number of derived resources that should be added to a pod
   */
  maxResources: number;
  /**
   * The string used to indicate that a derived resource quad term should be replaced
   * by a variable in the query
   */
  variableReplacementIndicator: string;
  /**
   * Filter determining whether a quad gets added to the cset. Used to, for example
   * restrict derived resources to quads with the pod's WebId / baseUrl as subject
   */
  filter?: IQuadMatcher;
  /**
   * Defines the authoritative scope of the Derived Resource.
   * * - `'baseUri'`: Only considers triples the resource is authoritative over.
   * The Derived Resource is considered authoritative
   * only for triples where the subject starts with the Pod's base URI. Non-matching
   * triples are filtered out to.
   * (e.g., A resource from /pods/alice is authoritative for `/pods/alice/posts`,
   * but triples about `pods/bob/posts found inside it are not considered).
   * * - `undefined`: No authority filtering. All triples contained in the Derived Resource
   * are accepted regardless of their subject.
   */
  authoritativenessStrategy?: 'baseUri';
}

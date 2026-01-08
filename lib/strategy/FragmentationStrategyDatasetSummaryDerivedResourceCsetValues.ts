import type * as RDF from '@rdfjs/types';
import type { IQuadSink } from '../io/IQuadSink';
import type { IQuadMatcher } from '../quadmatcher/IQuadMatcher';
import { DatasetSummaryDerivedResourceCset } from '../summary/DatasetSummaryDerivedResourceCset';
import { 
  FragmentationStrategyDatasetSummaryDerivedResource, 
  type IFragmentationStrategyDatasetSummaryDerivedResourceOptions 
} from './FragmentationStrategyDatasetSummaryDerivedResource';

export class FragmentationStrategyDatasetSummaryDerivedResourceCsetValues
  extends FragmentationStrategyDatasetSummaryDerivedResource<DatasetSummaryDerivedResourceCset> {
  protected readonly filter: IQuadMatcher | undefined;
  protected readonly selector: string = '*';

  protected readonly derivedResourceConstructionStrategy: 'minSize' | 'maxSize' | 'maxCardinality';
  protected readonly authoritativenessStrategy: 'baseUri' | undefined;

  protected readonly maxResources: number;

  protected readonly variableReplacementIndicator: string;

  public constructor(options: IFragmentationStrategyDatasetSummaryDerivedResourceCsetValuesOptions) {
    super(options);
    this.filter = options.filter;

    this.derivedResourceConstructionStrategy = options.derivedResourceConstructionStrategy;
    this.authoritativenessStrategy = options.authoritativenessStrategy;

    this.maxResources = options.maxResources;
    this.variableReplacementIndicator = options.variableReplacementIndicator;
  }

  protected createSummary(dataset: string): DatasetSummaryDerivedResourceCset {
    return new DatasetSummaryDerivedResourceCset(
      {
        dataset,
        derivedResourceConstructionStrategy: this.derivedResourceConstructionStrategy,
        maxResources: this.maxResources,
        variableReplacementIndicator: this.variableReplacementIndicator,
        filter: this.filter,
        authoritativenessStrategy: this.authoritativenessStrategy,
      },
    );
  }
  protected constructQuery(quads: RDF.Quad[], context: Record<string, any>){
    const predicates = quads.map(quad => `<${quad.predicate.value}>`);
    const query = 
`CONSTRUCT { ?s ?p ?o }
WHERE {
  ?s ?p ?o .
  VALUES ?p { ${predicates.join(' ')} }
}`;
    return query;
  }
}

export interface IFragmentationStrategyDatasetSummaryDerivedResourceCsetValuesOptions
  extends IFragmentationStrategyDatasetSummaryDerivedResourceOptions {
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

import type { IQuadMatcher } from '../quadmatcher/IQuadMatcher';
import { DatasetSummaryDerivedResourceStarTypes } from '../summary/DatasetSummaryDerivedResourceStarTypes';
import {
  FragmentationStrategyDatasetSummaryDerivedResource,
  type IFragmentationStrategyDatasetSummaryDerivedResourceOptions,
} from './FragmentationStrategyDatasetSummaryDerivedResource';

export class FragmentationStrategyDatasetSummaryDerivedResourceStarTypes
  extends FragmentationStrategyDatasetSummaryDerivedResource<DatasetSummaryDerivedResourceStarTypes> {
  protected readonly filter: IQuadMatcher | undefined;
  protected readonly selector: string = '*';

  protected readonly authoritativenessStrategy: 'baseUri' | undefined;
  protected readonly typePredicateMatcher: IQuadMatcher;

  protected readonly variableReplacementIndicator: string;

  public constructor(options: IFragmentationStrategyDatasetSummaryDerivedResourceStarTypesOptions) {
    super(options);
    this.filter = options.filter;

    this.authoritativenessStrategy = options.authoritativenessStrategy;
    this.typePredicateMatcher = options.typePredicateMatcher;

    this.variableReplacementIndicator = options.variableReplacementIndicator;
  }

  protected createSummary(dataset: string): DatasetSummaryDerivedResourceStarTypes {
    return new DatasetSummaryDerivedResourceStarTypes(
      {
        dataset,
        variableReplacementIndicator: this.variableReplacementIndicator,
        filter: this.filter,
        authoritativenessStrategy: this.authoritativenessStrategy,
        typePredicateMatcher: this.typePredicateMatcher,
      },
    );
  }
}

export interface IFragmentationStrategyDatasetSummaryDerivedResourceStarTypesOptions
  extends IFragmentationStrategyDatasetSummaryDerivedResourceOptions {
  /**
   * Matcher that should match all quads that denote a type
   */
  typePredicateMatcher: IQuadMatcher;
  /**
   * The string used to indicate that a derived resource quad term should be replaced
   * by a variable in the query
   */
  variableReplacementIndicator: string;
  /**
   * Filter determining whether a quad gets added to the summary. Used to, for example
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

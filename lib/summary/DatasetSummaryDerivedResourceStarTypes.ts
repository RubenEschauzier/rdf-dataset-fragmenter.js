import type * as RDF from '@rdfjs/types';
import { DataFactory } from 'rdf-data-factory';
import type { IQuadMatcher } from '../quadmatcher/IQuadMatcher';
import type { IDatasetSummaryArgs } from './DatasetSummary';
import { DatasetSummaryDerivedResource, type IDatasetSummarySparqlOutput } from './DatasetSummaryDerivedResource';

export class DatasetSummaryDerivedResourceStarTypes extends DatasetSummaryDerivedResource {
  private readonly types: Set<string>;
  private typePredicate: string | undefined;

  private readonly filter: IQuadMatcher | undefined;
  private readonly authoritativenessStrategy: 'baseUri' | undefined;
  private readonly variableReplacementIndicator: string;
  private readonly typePredicateMatcher: IQuadMatcher;
  // eslint-disable-next-line ts/naming-convention
  private readonly DF = new DataFactory();

  public constructor(args: IDatasetSummaryDerivedResourceStarTypesArgs) {
    super(args);
    this.types = new Set();

    this.authoritativenessStrategy = args.authoritativenessStrategy;

    this.variableReplacementIndicator = args.variableReplacementIndicator;

    this.filter = args.filter;
    this.typePredicateMatcher = args.typePredicateMatcher;
  }

  public register(quad: RDF.Quad): void {
    if (!this.filter || this.filter.matches(quad)) {
      if (this.authoritativenessStrategy === 'baseUri' &&
        !quad.subject.value.startsWith(this.dataset)) {
        return;
      }
      if (!this.typePredicateMatcher.matches(quad)) {
        return;
      }
      this.types.add(quad.object.value);
      this.typePredicate = quad.predicate.value;
    }
  }

  public serialize(): IDatasetSummarySparqlOutput {
    // Each type has its own query for all triples relating to that type
    if (!this.typePredicate) {
      return { quads: [], grouped: [], iri: this.dataset };
    }
    const selectedQuads: RDF.Quad[][] = [ ...this.types.values() ].map(type =>
      [
        this.DF.quad(
          this.DF.namedNode(`${this.variableReplacementIndicator}s`),
          this.DF.namedNode(this.typePredicate!),
          this.DF.namedNode(type),
        ),
      ]);
    const grouped = selectedQuads.map(quads => quads.length);
    return { quads: selectedQuads.flat(1), iri: this.dataset, grouped };
  }
}

export interface IDatasetSummaryDerivedResourceStarTypesArgs extends IDatasetSummaryArgs {
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
   * Filter determining whether a quad gets is considered for this summary
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

import type { IQuadSink } from '../io/IQuadSink';
import type { IQuadMatcher } from '../quadmatcher/IQuadMatcher';
import { DatasetSummaryCsetDerivedResource } from '../summary/DatasetSummaryCsetDerivedResource';
import { FragmentationStrategyDatasetSummary } from './FragmentationStrategyDatasetSummary';
import type { IFragmentationStrategyDatasetSummaryOptions } from './FragmentationStrategyDatasetSummary';

// TODO: Add Derived Resource indicator to pod
// TODO: Add a filter based on the subject partial match
export class FragmentationStrategyDatasetSummaryCsetDerivedResource
  extends FragmentationStrategyDatasetSummary<DatasetSummaryCsetDerivedResource> {
  protected readonly exclusionPatterns: RegExp[];
  protected readonly filter: IQuadMatcher | undefined;

  protected readonly derivedResourceConstructionStrategy: 'minSize' | 'maxCardinality';
  protected readonly authoritativenessStrategy: 'baseUri' | undefined;

  protected readonly maxResources: number;

  protected readonly variableReplacementIndicator: string;
  protected readonly derivedResourceFilename: string;

  public constructor(options: IFragmentationStrategyDatasetSummaryCsetDerivedResourceOptions) {
    super(options);
    this.exclusionPatterns = options.exclusionPatterns.map(exp => new RegExp(exp, 'u'));

    this.derivedResourceConstructionStrategy = options.derivedResourceConstructionStrategy;
    this.authoritativenessStrategy = options.authoritativenessStrategy;

    this.maxResources = options.maxResources;
    this.variableReplacementIndicator = options.variableReplacementIndicator;
    this.derivedResourceFilename = options.derivedResourceFilename;

    this.filter = options.filter;

    if (!this.derivedResourceFilename.includes(':COUNT:')) {
      throw new Error(
        `DerivedResourceFilename parameter does not contain :COUNT:, got ${this.derivedResourceFilename}`,
      );
    }
  }

  protected override subjectToDatasets(subject: string): Set<string> {
    for (const exclusion of this.exclusionPatterns) {
      if (exclusion.test(subject)) {
        return new Set();
      }
    }

    const mappings = new Set<string>();
    for (const exp of this.datasetPatterns) {
      const matches = exp.exec(subject);
      if (matches) {
        for (const match of matches) {
          mappings.add(match);
        }
      }
    }
    return mappings;
  }

  protected createSummary(dataset: string): DatasetSummaryCsetDerivedResource {
    return new DatasetSummaryCsetDerivedResource(
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

  protected override async flush(quadSink: IQuadSink): Promise<void> {
    this.processBlankNodes();
    for (const [ key, summary ] of this.summaries) {
      const output = summary.serialize();
      let startIdx = 0;
      let iriIdx = 0;
      for (const groupSize of output.grouped) {
        const quadsSingleResource = output.quads.slice(startIdx, startIdx + groupSize);
        const fileName = this.derivedResourceFilename.replace(':COUNT:', `${iriIdx}`);
        for (const quad of quadsSingleResource) {
          await quadSink.push(`${output.iri}${fileName}`, quad);
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
  /**
   * How the derived resource triple patterns should be selected from the candidate csets
   */
  derivedResourceConstructionStrategy: 'minSize' | 'maxCardinality';
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
   * Filename template for derived resource. Must contain :COUNT: which
   * will be incremented as more derived resources are added.
   * If it is in a directory it will create this directory
   */
  derivedResourceFilename: string;
  /**
   * What files should not be considered when creating derived resources
   */
  exclusionPatterns: string[];
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

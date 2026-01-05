import type * as RDF from '@rdfjs/types';
import type { IQuadSink } from '../io/IQuadSink';
import type { IMetadataGenerator } from '../metadata/IMetadataGenerator';
import { DF } from '../summary/DatasetSummary';
import type { IDatasetSummaryDerivedResource, IDatasetSummarySparqlOutput } from '../summary/DatasetSummaryDerivedResource';
import { FragmentationStrategyDatasetSummary } from './FragmentationStrategyDatasetSummary';
import type { IFragmentationStrategyDatasetSummaryOptions } from './FragmentationStrategyDatasetSummary';

export abstract class FragmentationStrategyDatasetSummaryDerivedResource<
  T extends IDatasetSummaryDerivedResource,
> extends FragmentationStrategyDatasetSummary<T> {
  protected readonly exclusionPatterns: RegExp[];
  protected readonly filterFilename: string;

  protected readonly metadataQuadsGenerator: IMetadataGenerator;

  protected readonly podTowebIds: Record<string, string> = {};
  protected readonly directMetadataLinkPredicate: string | undefined;
  protected readonly profilePredicateRegex: RegExp | undefined;
  protected readonly podBaseUriExtractionRegex: RegExp | undefined;

  public constructor(options: IFragmentationStrategyDatasetSummaryDerivedResourceOptions) {
    super(options);
    this.exclusionPatterns = options.exclusionPatterns.map(exp => new RegExp(exp, 'u'));
    this.filterFilename = options.filterFilename;
    this.metadataQuadsGenerator = options.metadataQuadsGenerator;

    this.checkAllOrNone({
      directMetadataLinkPredicate: options.directMetadataLinkPredicate,
      profilePredicateRegex: options.profilePredicateRegex,
      podBaseUriExtractionRegex: options.podBaseUriExtractionRegex,
    });

    this.directMetadataLinkPredicate = options.directMetadataLinkPredicate;
    if (options.profilePredicateRegex && options.podBaseUriExtractionRegex) {
      this.profilePredicateRegex = new RegExp(options.profilePredicateRegex, 'u');
      this.podBaseUriExtractionRegex = new RegExp(options.podBaseUriExtractionRegex, 'u');
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

  protected override async handleQuad(quad: RDF.Quad): Promise<void> {
    await super.handleQuad(quad);
    // Including direct metadata links requires storing all references to a
    // webId we find.
    if (this.directMetadataLinkPredicate && this.profilePredicateRegex &&
      this.profilePredicateRegex.test(quad.predicate.value)
    ) {
      const matches = this.podBaseUriExtractionRegex!.exec(quad.object.value);
      if (matches) {
        for (const match of matches) {
          this.podTowebIds[match] = quad.object.value;
        }
      }
    }
  }

  /**
   * Writes the quads required to register the generated derived resource in the pod
   * @param output The serialization output of the derived resource
   * @param quadSink Quad sink to write to
   * @param metaFile The iri of the .meta file of the given derived resource
   */
  protected async writeMetaFile(
    output: IDatasetSummarySparqlOutput,
    quadSink: IQuadSink,
    metaFile: string
  ){
    const metadataQuads = this.metadataQuadsGenerator.generateMetadata({
      podUri: output.iri,
      selectorPattern: `${output.iri}*`,
      filterFilenameTemplate: this.filterFilename,
      nResources: output.grouped.length,
    });

    for (const quad of metadataQuads) {
      await quadSink.push(metaFile, quad);
    }
  }

  /**
   * Write a predicate in the pod's WebId file that points directly to
   * where the derived resources of that pod are defined.
   * @param output The serialization output of the derived resource
   * @param quadSink Quad sink to write to
   * @param metaFile The iri of the .meta file of the given derived resource
   */
  protected async writeDirectMetadataLink(
    output: IDatasetSummarySparqlOutput,
    quadSink: IQuadSink,
    metaFile: string
  ){
    const podMatches = this.podBaseUriExtractionRegex!.exec(output.iri);
    if (podMatches) {
      for (const match of podMatches) {
        const summaryWebId = this.podTowebIds[match];
        if (!summaryWebId) {
          throw new Error(`Found summary for pod without registered WebId: ${match}`);
        }
        const metaQuad = DF.quad(
          DF.namedNode(summaryWebId),
          DF.namedNode(this.directMetadataLinkPredicate!),
          DF.namedNode(metaFile),
        );
        await quadSink.push(summaryWebId, metaQuad);
      }
    }
  }

  protected override async flush(quadSink: IQuadSink): Promise<void> {
    this.processBlankNodes();
    for (const [ key, summary ] of this.summaries) {
      const output = summary.serialize();

      let startIdx = 0;
      let iriIdx = 0;
      for (const groupSize of output.grouped) {
        const quadsSingleResource = output.quads.slice(startIdx, startIdx + groupSize);
        const fileName = this.filterFilename.replace(':COUNT:', `${iriIdx}`);
        for (const quad of quadsSingleResource) {
          await quadSink.push(`${output.iri}${fileName}`, quad);
        }
        startIdx += groupSize;
        iriIdx++;
      }
      const metaFile = `${output.iri}${this.metadataQuadsGenerator.getMetaFileName()}`;
      this.writeMetaFile(output, quadSink, metaFile);

      if (this.directMetadataLinkPredicate) {
        this.writeDirectMetadataLink(output, quadSink, metaFile);
      }
      this.summaries.delete(key);
    }
    await super.flush(quadSink);
  }

  protected checkAllOrNone<T>(
    values: Record<string, T | undefined>,
    context = 'Configuration',
  ): void {
    const defined = Object.entries(values).filter(([ , v ]) => v !== undefined);
    const undefinedOnes = Object.entries(values).filter(([ , v ]) => v === undefined);

    if (defined.length > 0 && undefinedOnes.length > 0) {
      throw new Error(
        `${context} error: either all of [${Object.keys(values).join(', ')}] must be defined or all must be undefined. ` +
        `Defined: ${defined.map(([ k ]) => k).join(', ')}; ` +
        `Missing: ${undefinedOnes.map(([ k ]) => k).join(', ')}.`,
      );
    }
  }
}

export interface IFragmentationStrategyDatasetSummaryDerivedResourceOptions
  extends IFragmentationStrategyDatasetSummaryOptions {
  /**
   * Make derived resource for all star-shaped types, make derived resource for all star-shaped
   * entities sets without type (so it shows what predicates form an entity). If query gives a type it can
   * just use type-based derived resources.
   * If the engine has no type it can try to infer what type queries it should issue.
   * Proceed as follows check all star-shaped predicate sets without type, if the query shape
   * is a subset of a predicate set without types we cannot use any derived resource.
   * If not, we can determine the types to query by downloading
   */
  /**
   * Metadata file construction method
   * TODO: Change cset to enumerate all predicates of an entity of that cset, not join them. Also consider
   * hierarchical nature of csets
   * This acts as a sort of index instead. The engine if it has a subset of those triples can just download
   * this subset quickly filter out non-matching predicates and use this to make the joins.
   * TODO: Add void descriptions showing what predicates are answered in a derived resource (Later)
   */
  metadataQuadsGenerator: IMetadataGenerator;
  /**
   * What files should not be considered when creating derived resources
   */
  exclusionPatterns: string[];
  /**
   * Filename template where filters will be stored
   */
  filterFilename: string;
  /**
   * If defined this derived resource class will include a direct link to the .meta file
   * containing the derived resource specification using the provided predicate
   */
  directMetadataLinkPredicate?: string;
  /**
   * Predicate regex for finding WebId URIs
   */
  profilePredicateRegex?: string;
  /**
   * Regex to extract pod base URI from a given URI
   */
  podBaseUriExtractionRegex?: string;
}

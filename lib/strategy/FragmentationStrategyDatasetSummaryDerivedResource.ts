import { writeFile } from 'node:fs/promises';
import { dirname } from 'node:path';
import type * as RDF from '@rdfjs/types';
import { mkdirp } from 'mkdirp';

import type { IQuadSink } from '../io/IQuadSink';
import type { IMetadataGenerator } from '../metadata/IMetadataGenerator';
import { DF } from '../summary/DatasetSummary';
import type {
  IDatasetSummaryDerivedResource,
  IDatasetSummarySparqlOutput,
} from '../summary/DatasetSummaryDerivedResource';
import { FragmentationStrategyDatasetSummary } from './FragmentationStrategyDatasetSummary';
import type { IFragmentationStrategyDatasetSummaryOptions } from './FragmentationStrategyDatasetSummary';

export abstract class FragmentationStrategyDatasetSummaryDerivedResource<
  T extends IDatasetSummaryDerivedResource,
> extends FragmentationStrategyDatasetSummary<T> {
  protected readonly exclusionPatterns: RegExp[];
  protected readonly filterFilename: string;

  protected readonly metadataQuadsGenerator: IMetadataGenerator;
  protected readonly selectorPatterns: string[];

  protected readonly iriToPath: Map<RegExp, string>;

  protected readonly podTowebIds: Record<string, string> = {};
  protected readonly directMetadataLinkPredicate: string | undefined;
  protected readonly profilePredicateRegex: RegExp | undefined;
  protected readonly podBaseUriExtractionRegex: RegExp | undefined;

  protected readonly fileMetadataLinkPredicate: string | undefined;
  /**
   * The document IRIs encountered per dataset.
   * Only tracked when fileMetadataLinkPredicate is defined.
   */
  protected readonly datasetToFiles: Map<string, Set<string>> = new Map();

  public constructor(options: IFragmentationStrategyDatasetSummaryDerivedResourceOptions) {
    super(options);
    this.exclusionPatterns = options.exclusionPatterns.map(exp => new RegExp(exp, 'u'));
    this.filterFilename = options.filterFilename;
    this.metadataQuadsGenerator = options.metadataQuadsGenerator;
    this.selectorPatterns = options.selectorPatterns;

    this.iriToPath = new Map(Object.entries(options.iriToPath).map(([ exp, sub ]) => [
      new RegExp(exp, 'u'),
      sub,
    ]));

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

    this.fileMetadataLinkPredicate = options.fileMetadataLinkPredicate;
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
        for (const match of new Set(matches)) {
          this.podTowebIds[match] = quad.object.value;
        }
      }
    }
    // Linking every file of a dataset to the metadata file requires knowing all
    // documents that make up that dataset.
    if (this.fileMetadataLinkPredicate && quad.subject.termType === 'NamedNode') {
      const file = this.toDocumentIri(quad.subject.value);
      for (const dataset of this.subjectToDatasets(quad.subject.value)) {
        let files = this.datasetToFiles.get(dataset);
        if (!files) {
          files = new Set();
          this.datasetToFiles.set(dataset, files);
        }
        files.add(file);
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
    iri: string,
    nResources: number,
    quadSink: IQuadSink,
    metaFile: string,
    context?: Record<string,any>
  ): Promise<void> {
    const metadataQuads = this.metadataQuadsGenerator.generateMetadata({
      podUri: iri,
      selectorPatterns: this.selectorPatterns.map(pattern => `${iri}${pattern}`),
      filterFilenameTemplate: this.filterFilename,
      nResources,
      context
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
    metaFile: string,
  ): Promise<void> {
    const podMatches = this.podBaseUriExtractionRegex!.exec(output.iri);
    if (podMatches) {
      // The regex captures the pod base URI, so the full match and the capture group
      // are the same string. Only write the link once per distinct pod base URI.
      for (const match of new Set(podMatches)) {
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

  /**
   * Link every document of a dataset to the metadata file describing the derived resources
   * that dataset exposes.
   * @param dataset The dataset (pod) the documents belong to
   * @param quadSink Quad sink to write to
   * @param metaFile The iri of the .meta file of the given derived resource
   */
  protected async writeFileMetadataLinks(
    dataset: string,
    quadSink: IQuadSink,
    metaFile: string,
  ): Promise<void> {
    const files = this.datasetToFiles.get(dataset);
    if (!files) {
      return;
    }
    for (const file of files) {
      await quadSink.push(file, DF.quad(
        DF.namedNode(file),
        DF.namedNode(this.fileMetadataLinkPredicate!),
        DF.namedNode(metaFile),
      ));
    }
    this.datasetToFiles.delete(dataset);
  }

  /**
   * The IRI of the document a term is part of, i.e., the IRI without its hash fragment.
   */
  protected toDocumentIri(iri: string): string {
    const posHash = iri.indexOf('#');
    return posHash >= 0 ? iri.slice(0, posHash) : iri;
  }

  protected getFilePath(iri: string): string {
    // Remove hash fragment
    const posHash = iri.indexOf('#');
    if (posHash >= 0) {
      iri = iri.slice(0, posHash);
    }

    // Find base path from the first matching baseIRI
    let bestMatch: RegExpExecArray | undefined;
    let bestRegex: RegExp | undefined;

    for (const exp of this.iriToPath.keys()) {
      const match = exp.exec(iri);
      if (match && (bestMatch === undefined || match[0].length > bestMatch[0].length)) {
        bestMatch = match;
        bestRegex = exp;
      }
    }

    // Crash if we did not find a matching baseIRI
    if (!bestRegex) {
      throw new Error(`No IRI mapping found for ${iri}`);
    }

    // Perform substitution and replace illegal directory names
    let path = iri.replace(bestRegex, this.iriToPath.get(bestRegex)!);

    // Replace illegal directory names
    path = path.replaceAll(/[*|"<>?:]/ug, '_');
    return path;
  }

  protected async writeDirAndFile(path: string, data: string, encoding: BufferEncoding): Promise<void> {
    await mkdirp(dirname(path));
    await writeFile(path, data, encoding);
  }

  protected override async flush(quadSink: IQuadSink): Promise<void> {
    this.processBlankNodes();
    for (const [ key, summary ] of this.summaries) {
      const output = summary.serialize();

      let startIdx = 0;
      let iriIdx = 0;
      for (const groupSize of output.grouped) {
        const quadsSingleResource = output.quads.slice(startIdx, startIdx + groupSize);
        const constructQuery = this.constructQuery(quadsSingleResource, {});

        const filePathPod = this.getFilePath(output.iri);
        const path = `${filePathPod}${this.filterFilename.replace(':COUNT:', `${iriIdx}`)}.rq`;
        await this.writeDirAndFile(path, constructQuery.query, 'utf-8');

        startIdx += groupSize;
        iriIdx++;
      }
      const metaFile = `${output.iri}${this.metadataQuadsGenerator.getMetaFileName()}`;
      await this.writeMetaFile(output.iri, output.grouped.length, quadSink, metaFile);

      if (this.directMetadataLinkPredicate) {
        await this.writeDirectMetadataLink(output, quadSink, metaFile);
      }
      if (this.fileMetadataLinkPredicate) {
        await this.writeFileMetadataLinks(key, quadSink, metaFile);
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
        `${context} error: either all of [${Object.keys(values).join(', ')}] ` +
        `must be defined or all must be undefined. ` +
        `Defined: ${defined.map(([ k ]) => k).join(', ')}; ` +
        `Missing: ${undefinedOnes.map(([ k ]) => k).join(', ')}.`,
      );
    }
  }

  /**
   * Given summary serialization output and selected quads create the desired query
   */
  protected abstract constructQuery(quads: RDF.Quad[], context: Record<string, any>): IConstructQueryOutput;
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
   * TODO: Add a derived resource that is purely a queries of increasing size
   * TODO: Determine if our approach can also produce .meta files for specific resources and if
   * we do so correctly
   * TODO: Add void descriptions showing what predicates are answered in a derived resource (Later)
   */
  metadataQuadsGenerator: IMetadataGenerator;
  /**
   * Mapping of regular expressions to their replacements,
   * for determining the file path from a given IRI.
   * @range {json}
   */
  iriToPath: Record<string, string>;
  /**
   * What selector patterns should be defined for the given derived resource.
   */
  selectorPatterns: string[];
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
  /**
   * If defined, every document of a dataset gets a triple with this predicate pointing to the
   * .meta file that specifies the derived resources of that dataset. 
   * Should only be enabled on a single strategy, as every strategy that enables it writes
   * its own copy of the triple.
   */
  fileMetadataLinkPredicate?: string;
}


export interface IConstructQueryOutput{
  query: string,
  metadata?: Record<string, any>
}
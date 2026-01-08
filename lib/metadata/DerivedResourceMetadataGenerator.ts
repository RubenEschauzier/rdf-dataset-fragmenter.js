import type * as RDF from '@rdfjs/types';
import { DataFactory } from 'rdf-data-factory';
import type { IMetadataGenerationInput, IMetadataGenerator } from './IMetadataGenerator';

/**
 * Generates metadata for derived resources in a Solid pod
 */
export class DerivedResourceMetadataGenerator implements IMetadataGenerator {
  // eslint-disable-next-line ts/naming-convention
  private readonly DF = new DataFactory();
  private readonly derivedNamespace: string;
  private readonly templatesTemplate: string;
  private readonly metaFilename: string;

  public constructor(options: IDerivedResourcesMetadataGeneratorOptions) {
    this.derivedNamespace = options.derivedNamespace;
    this.metaFilename = options.metaFilename;
    this.templatesTemplate = options.templatesTemplate;

    if (!this.templatesTemplate.includes(':COUNT:')) {
      throw new Error(
        `Template filenames do not contain :COUNT:`,
      );
    }
  }

  public generateMetadata(input: IMetadataGenerationInput): RDF.Quad[] {
    const quads: RDF.Quad[] = [];
    const podNode = this.DF.namedNode(input.podUri);

    for (let i = 0; i < input.nResources; i++) {
      const descriptorNode = this.DF.blankNode();
      quads.push(this.DF.quad(
        podNode,
        this.DF.namedNode(`${this.derivedNamespace}derivedResource`),
        descriptorNode,
      ));
      const template = this.templatesTemplate.replace(':COUNT:', `${i}`);
      const filter = `${input.podUri}${input.filterFilenameTemplate.replace(':COUNT:', `${i}`)}`;
      quads.push(this.DF.quad(
        descriptorNode,
        this.DF.namedNode(`${this.derivedNamespace}template`),
        this.DF.literal(template),
      ));
      for (const selectorPattern of input.selectorPatterns){
        quads.push(this.DF.quad(
          descriptorNode,
          this.DF.namedNode(`${this.derivedNamespace}selector`),
          this.DF.namedNode(selectorPattern),
        ));
      }
      quads.push(this.DF.quad(
        descriptorNode,
        this.DF.namedNode(`${this.derivedNamespace}filter`),
        this.DF.namedNode(filter),
      ));
    }
    return quads;
  }

  public getMetaFileName(): string {
    return this.metaFilename;
  }
}

export interface IDerivedResourcesMetadataGeneratorOptions {
  /**
   * Prefix of predicates related to derived resources
   */
  derivedNamespace: string;
  /**
   * Where the metadata file should be stored
   */
  metaFilename: string;
  /**
   * Indicator where the actual derived resource will be stored
   */
  templatesTemplate: string;
}

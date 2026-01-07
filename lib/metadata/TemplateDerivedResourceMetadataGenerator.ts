import type * as RDF from '@rdfjs/types';
import { DataFactory } from 'rdf-data-factory';
import type { IMetadataGenerationInput, IMetadataGenerator } from './IMetadataGenerator';

/**
 * Generates metadata for templated derived resources, it assumes the template names are 
 * constant and only a counts increments to differentiate them.
 */
export class TemplateDerivedResourceMetadataGenerator implements IMetadataGenerator {
  // eslint-disable-next-line ts/naming-convention
  private readonly DF = new DataFactory();
  private readonly derivedNamespace: string;
  private readonly templatesTemplate: string;
  private readonly metaFilename: string;
  private readonly variableTemplate: string;

  public constructor(options: ITemplateDerivedResourcesMetadataGeneratorOptions) {
    this.derivedNamespace = options.derivedNamespace;
    this.metaFilename = options.metaFilename;
    this.templatesTemplate = options.templatesTemplate;
    this.variableTemplate = options.variableTemplate;

    if (!this.templatesTemplate.includes(':COUNT:')) {
      throw new Error(
        `Template filenames do not contain :COUNT:`,
      );
    }

    if (!this.variableTemplate.includes(':COUNT:')) {
      throw new Error(
        `Variable template does not contain :COUNT:`,
      );
    }
  }

  public generateMetadata(input: IMetadataGenerationInput): RDF.Quad[] {
    const quads: RDF.Quad[] = [];
    const podNode = this.DF.namedNode(input.podUri);

    for (let i = 1; i <= input.nResources; i++) {
      const descriptorNode = this.DF.blankNode();

      quads.push(this.DF.quad(
        podNode,
        this.DF.namedNode(`${this.derivedNamespace}derivedResource`),
        descriptorNode,
      ));

      let templateString = this.templatesTemplate.replace(':COUNT:', `${i}`);

      const variableSegments: string[] = [];
      for (let v = 1; v <= i; v++) {
        const varName = this.variableTemplate.replace(':COUNT:', `${v}`);
        
        variableSegments.push(`{${varName}}`);
      }

      const separator = templateString.endsWith('/') ? '' : '/';
      templateString += `${separator}${variableSegments.join('/')}`;

      quads.push(this.DF.quad(
        descriptorNode,
        this.DF.namedNode(`${this.derivedNamespace}template`),
        this.DF.literal(templateString),
      ));

      quads.push(this.DF.quad(
        descriptorNode,
        this.DF.namedNode(`${this.derivedNamespace}selector`),
        this.DF.namedNode(input.selectorPattern),
      ));

      const filter = `${input.podUri}${input.filterFilenameTemplate.replace(':COUNT:', `${i}`)}`;
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

export interface ITemplateDerivedResourcesMetadataGeneratorOptions {
  /**
   * Prefix of predicates related to derived resources
   */
  derivedNamespace: string;
  /**
   * Where the metadata file should be stored
   */
  metaFilename: string;
  /**
   * Indicator where the actual derived resource will be stored.
   * Example: "derived/ladder/:COUNT:"
   */
  templatesTemplate: string;
  /**
   * Template for the variable names appended to the URL.
   * Must contain ':COUNT:' to be replaced by the variable index.
   * Example: "p:COUNT:" results in URL variables {p1}, {p2}, etc.
   */
  variableTemplate: string;
}
import type * as RDF from '@rdfjs/types';

/**
 * Interface for metadata generation strategies
 */
export interface IMetadataGenerator {
  /**
   * Generate metadata quads for derived resources
   * @param input - The context containing information needed to generate metadata
   * @returns Array of quads to be written to the .meta file
   */
  generateMetadata: (input: IMetadataGenerationInput) => RDF.Quad[];
  getMetaFileName: () => string;
}

/**
 * Context information needed to generate metadata
 */
export interface IMetadataGenerationInput {
  /**
   * Base URI of the pod
   */
  podUri: string;
  /**
   * Relative selector path. For example '*' simply selects all
   * data in a pod for the given derived resource.
   */
  selectorPattern: string;
  /**
   * Template for filter filename
   */
  filterFilenameTemplate: string;
  /**
   * Number of resources to generate metadata for
   */
  nResources: number;
  /**
   * Optional context
   */
  context?: Record<string, any>;
}

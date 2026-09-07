import type { Quad } from '@rdfjs/types';

import type { IQuadSink } from '../io/IQuadSink';
import { DatasetSummaryDerivedResourceStub } from '../summary/DatasetSummaryDerivedResourceStub';
import {
  FragmentationStrategyDatasetSummaryDerivedResource,
  type IConstructQueryOutput,
  type IFragmentationStrategyDatasetSummaryDerivedResourceOptions,
} from './FragmentationStrategyDatasetSummaryDerivedResource';

/**
 * Base class for derived resources that expose a ladder of fully parameterized queries of a
 * fixed shape and increasing size. The shape itself is provided by the subclasses.
 * Can generate both SELECT and CONSTRUCT queries
 * */
export abstract class FragmentationStrategyDatasetSummaryDerivedResourceTemplate
  extends FragmentationStrategyDatasetSummaryDerivedResource<DatasetSummaryDerivedResourceStub> {
  protected readonly minSizeConstruct: number;
  protected readonly maxSizeConstruct: number;
  protected readonly minSizeSelect: number;
  protected readonly maxSizeSelect: number;

  /**
   * The queries to generate per dataset, in the order they are numbered in the filter
   * filenames and the metadata file.
   */
  protected readonly querySpecs: ITemplateQuerySpec[];

  public constructor(options: IFragmentationStrategyDatasetSummaryDerivedResourceTemplateOptions) {
    super(options);
    this.minSizeConstruct = options.minSizeConstruct ?? 1;
    this.maxSizeConstruct = options.maxSizeConstruct ?? 0;
    this.minSizeSelect = options.minSizeSelect ?? 1;
    this.maxSizeSelect = options.maxSizeSelect ?? 0;

    if (this.minSizeConstruct < 1 || this.minSizeSelect < 1) {
      throw new Error(`Template query sizes must be at least 1`);
    }

    this.querySpecs = [
      ...this.sizeRange(this.minSizeConstruct, this.maxSizeConstruct)
        .map(size => ({ size, queryType: 'construct' as const })),
      ...this.sizeRange(this.minSizeSelect, this.maxSizeSelect)
        .map(size => ({ size, queryType: 'select' as const })),
    ];

    if (this.querySpecs.length === 0) {
      throw new Error(`No template queries to generate: both the CONSTRUCT and SELECT size range are empty`);
    }
  }

  protected createSummary(dataset: string): DatasetSummaryDerivedResourceStub {
    return new DatasetSummaryDerivedResourceStub({
      dataset,
    });
  }

  protected override async flush(quadSink: IQuadSink): Promise<void> {
    this.processBlankNodes();
    for (const [ key, summary ] of this.summaries) {
      const output = summary.serialize();
      const queryTemplateNames: string[][] = [];
      for (const [ index, spec ] of this.querySpecs.entries()) {
        const constructQuery = this.constructQuery(output.quads, spec);
        queryTemplateNames.push(constructQuery.metadata!.templateNames);

        const filePathPod = this.getFilePath(output.iri);
        const path = `${filePathPod}${this.filterFilename.replace(':COUNT:', `${index + 1}`)}.rq`;

        await this.writeDirAndFile(path, constructQuery.query, 'utf-8');
      }
      const metaFile = `${output.iri}${this.metadataQuadsGenerator.getMetaFileName()}`;
      await this.writeMetaFile(
        output.iri,
        this.querySpecs.length,
        quadSink,
        metaFile,
        { parameterNames: queryTemplateNames },
      );

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

  // eslint-disable-next-line unused-imports/no-unused-vars
  protected constructQuery(quads: Quad[], context: Record<string, any>): IConstructQueryOutput {
    const { size, queryType } = <ITemplateQuerySpec> context;
    const { patterns, templateNames } = this.shapePatterns(size);
    const queryBody = patterns.map(pattern => `  ${pattern}`).join('\n');

    const query = queryType === 'construct' ?
        `CONSTRUCT {
${queryBody}
}
WHERE {
${queryBody}
}` :
        `SELECT * WHERE {
${queryBody}
}`;
    return { query, metadata: { templateNames }};
  }

  /**
   * All sizes in the inclusive range, or an empty list if the range is empty.
   */
  protected sizeRange(minSize: number, maxSize: number): number[] {
    const sizes: number[] = [];
    for (let size = minSize; size <= maxSize; size++) {
      sizes.push(size);
    }
    return sizes;
  }

  /**
   * Produce the triple patterns of this shape with the given number of predicates.
   * @param size The number of predicates (and with that, triple patterns) in the shape
   */
  protected abstract shapePatterns(size: number): IShapePatterns;
}

export interface ITemplateQuerySpec {
  /**
   * The number of predicates in the query shape.
   */
  size: number;
  /**
   * The projection clause used for the query.
   */
  queryType: 'construct' | 'select';
}

export interface IShapePatterns {
  /**
   * The triple patterns of the shape, without indentation and terminated by a dot.
   */
  patterns: string[];
  /**
   * All template names used in the patterns, in the order they should be bound in the URL template.
   */
  templateNames: string[];
}

export interface IFragmentationStrategyDatasetSummaryDerivedResourceTemplateOptions
  extends IFragmentationStrategyDatasetSummaryDerivedResourceOptions {
  /**
   * The smallest query size emitted as a CONSTRUCT query. Defaults to 1.
   */
  minSizeConstruct?: number;
  /**
   * The largest query size emitted as a CONSTRUCT query. Defaults to 0, which emits no
   * CONSTRUCT queries.
   */
  maxSizeConstruct?: number;
  /**
   * The smallest query size emitted as a SELECT query. Defaults to 1.
   */
  minSizeSelect?: number;
  /**
   * The largest query size emitted as a SELECT query. Defaults to 0, which emits no
   * SELECT queries.
   */
  maxSizeSelect?: number;
}

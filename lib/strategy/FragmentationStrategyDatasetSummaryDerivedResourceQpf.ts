import { Quad } from '@rdfjs/types';
import type { IQuadSink } from '../io/IQuadSink';
import { DatasetSummaryDerivedResourceStub } from '../summary/DatasetSummaryDerivedResourceStub';
import { 
  FragmentationStrategyDatasetSummaryDerivedResource, 
  type IFragmentationStrategyDatasetSummaryDerivedResourceOptions 
} from './FragmentationStrategyDatasetSummaryDerivedResource';

export class FragmentationStrategyDatasetSummaryDerivedResourceQpf
  extends FragmentationStrategyDatasetSummaryDerivedResource<DatasetSummaryDerivedResourceStub> {
  public constructor(options: IFragmentationStrategyDatasetSummaryDerivedResourceOptions) {
    super(options);
  }

  protected createSummary(dataset: string): DatasetSummaryDerivedResourceStub {
    return new DatasetSummaryDerivedResourceStub(
      {
        dataset,
      },
    );
  }

  /**
   * Instead of writing quads, we simply write a filter text file with qpf in it
   * see (https://github.com/SolidLabResearch/derived-resources-component/blob/main/documentation/filters.md)
   * and we generate the required metadata quads for the .meta file
   * @param quadSink unused
   */
  protected override async flush(quadSink: IQuadSink): Promise<void> {
    for (const [ key, summary ] of this.summaries) {
      const output = summary.serialize();
      const filePathPod = this.getFilePath(output.iri);
      const path = `${filePathPod}${this.filterFilename.replace(':COUNT:', '0')}$.txt`;
      await this.writeDirAndFile(path, this.constructQuery(output.quads, {}), 'utf-8');

      const metaFile = `${output.iri}${this.metadataQuadsGenerator.getMetaFileName()}`;
      await this.writeMetaFile(output.iri, 1, quadSink, metaFile);

      if (this.directMetadataLinkPredicate) {
        await this.writeDirectMetadataLink(output, quadSink, metaFile);
      }

      this.summaries.delete(key);
    }
  }

  /**
   * Stub implementation that always returns "qpf"
   * @param quads Unused
   * @param context Unused
   * @returns "qpf"
   */
  protected constructQuery(quads: Quad[], context: Record<string, any>): string {
    return "qpf";
  }
}

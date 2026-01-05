import type { IQuadSink } from '../io/IQuadSink';
import { DatasetSummaryDerivedResourceQpf } from '../summary/DatasetSummaryDerivedResourceQpf';
import { 
  FragmentationStrategyDatasetSummaryDerivedResourceFileWriter,
  IFragmentationStrategyDatasetSummaryDerivedResourceFileWriterOptions
} from './FragmentationStrategyDatasetSummaryDerivedResourceFileWriter';

export class FragmentationStrategyDatasetSummaryDerivedResourceQpf
  extends FragmentationStrategyDatasetSummaryDerivedResourceFileWriter<DatasetSummaryDerivedResourceQpf> {

  public constructor(options: IFragmentationStrategyDatasetSummaryDerivedResourceFileWriterOptions) {
    super(options);
  }

  protected createSummary(dataset: string): DatasetSummaryDerivedResourceQpf {
    return new DatasetSummaryDerivedResourceQpf(
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
      const path = `${filePathPod}${this.filterFilename.replace(':COUNT:', '0')}.txt`;
      await this.writeDirAndFile(path, 'qpf', 'utf-8')

      const metaFile = `${output.iri}${this.metadataQuadsGenerator.getMetaFileName()}`;
      this.writeMetaFile(output, quadSink, metaFile);

      if (this.directMetadataLinkPredicate) {
        this.writeDirectMetadataLink(output, quadSink, metaFile);
      }

      // // Generate and write metadata quads that point towards the qpf resource
      // const metadataQuads = this.metadataQuadsGenerator.generateMetadata({
      //   podUri: iri,
      //   selectorPattern: `${iri}*`,
      //   filterFilenameTemplate: this.filterFilename,
      //   nResources: 1,
      // });

      // const metaFile = `${iri}${this.metadataQuadsGenerator.getMetaFileName()}`;
      // for (const quad of metadataQuads) {
      //   await quadSink.push(metaFile, quad);
      // }
      this.summaries.delete(key);
    }
  }
}
import type { IShapePatterns } from './FragmentationStrategyDatasetSummaryDerivedResourceTemplate';
import {
  FragmentationStrategyDatasetSummaryDerivedResourceTemplate,
  type IFragmentationStrategyDatasetSummaryDerivedResourceTemplateOptions,
} from './FragmentationStrategyDatasetSummaryDerivedResourceTemplate';

/**
 * Derived resources for star-shaped queries, i.e., all triple patterns share the same subject:
 * `$s$ $p1$ $o1$ . $s$ $p2$ $o2$ . ...`
 */
export class FragmentationStrategyDatasetSummaryDerivedResourceStarTemplate
  extends FragmentationStrategyDatasetSummaryDerivedResourceTemplate {
  public constructor(options: IFragmentationStrategyDatasetSummaryDerivedResourceTemplateOptions) {
    super(options);
  }

  protected shapePatterns(size: number): IShapePatterns {
    const patterns: string[] = [];
    const templateNames: string[] = [ '$s$' ];
    for (let i = 1; i <= size; i++) {
      const predTemplate = `$p${i}$`;
      const objTemplate = `$o${i}$`;
      patterns.push(`$s$ ${predTemplate} ${objTemplate} .`);
      templateNames.push(predTemplate, objTemplate);
    }
    return { patterns, templateNames };
  }
}

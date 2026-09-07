import type { IShapePatterns } from './FragmentationStrategyDatasetSummaryDerivedResourceTemplate';
import {
  FragmentationStrategyDatasetSummaryDerivedResourceTemplate,
  type IFragmentationStrategyDatasetSummaryDerivedResourceTemplateOptions,
} from './FragmentationStrategyDatasetSummaryDerivedResourceTemplate';

/**
 * Derived resources for linear (path-shaped) queries, i.e., the object of each triple pattern is
 * the subject of the next one: `$s$ $p1$ $o1$ . $o1$ $p2$ $o2$ . ...`
 */
export class FragmentationStrategyDatasetSummaryDerivedResourceLinearTemplate
  extends FragmentationStrategyDatasetSummaryDerivedResourceTemplate {
  public constructor(options: IFragmentationStrategyDatasetSummaryDerivedResourceTemplateOptions) {
    super(options);
  }

  protected shapePatterns(size: number): IShapePatterns {
    const patterns: string[] = [];
    const templateNames: string[] = [ '$s$' ];
    let subjTemplate = '$s$';
    for (let i = 1; i <= size; i++) {
      const predTemplate = `$p${i}$`;
      const objTemplate = `$o${i}$`;
      patterns.push(`${subjTemplate} ${predTemplate} ${objTemplate} .`);
      templateNames.push(predTemplate, objTemplate);
      subjTemplate = objTemplate;
    }
    return { patterns, templateNames };
  }
}

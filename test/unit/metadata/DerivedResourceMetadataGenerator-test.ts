import type * as RDF from '@rdfjs/types';
import { DerivedResourceMetadataGenerator } from '../../../lib/metadata/DerivedResourceMetadataGenerator';
import { TemplateDerivedResourceMetadataGenerator } from '../../../lib/metadata/TemplateDerivedResourceMetadataGenerator';

const NS = 'urn:npm:solid:derived-resources:';
const POD = 'http://example.org/pod/';

function filterObjects(quads: RDF.Quad[]): RDF.Term[] {
  return quads.filter(quad => quad.predicate.value === `${NS}filter`).map(quad => quad.object);
}

describe('DerivedResourceMetadataGenerator', () => {
  const generator = new DerivedResourceMetadataGenerator({
    derivedNamespace: NS,
    metaFilename: '.meta',
    templatesTemplate: 'resources/resource-patterns-:COUNT:',
  });
  const input = {
    podUri: POD,
    selectorPatterns: [ `${POD}**/*.nq` ],
    filterFilenameTemplate: 'filters/filter-patterns-:COUNT:',
    nResources: 2,
  };

  it('refers to the filter files by default', () => {
    expect(filterObjects(generator.generateMetadata(input)).map(term => [ term.termType, term.value ])).toEqual([
      [ 'NamedNode', `${POD}filters/filter-patterns-0` ],
      [ 'NamedNode', `${POD}filters/filter-patterns-1` ],
    ]);
  });

  it('writes the filters as literals when they are given', () => {
    const quads = generator.generateMetadata({ ...input, filters: [ 'patterns', 'qpf' ]});
    expect(filterObjects(quads).map(term => [ term.termType, term.value ])).toEqual([
      [ 'Literal', 'patterns' ],
      [ 'Literal', 'qpf' ],
    ]);
    expect(quads.filter(quad => quad.predicate.value === `${NS}template`).map(quad => quad.object.value))
      .toEqual([ 'resources/resource-patterns-0', 'resources/resource-patterns-1' ]);
  });
});

describe('TemplateDerivedResourceMetadataGenerator', () => {
  const generator = new TemplateDerivedResourceMetadataGenerator({
    derivedNamespace: NS,
    metaFilename: '.meta',
    templatesTemplate: 'resources/resource-star-template-:COUNT:',
    variableTemplate: 'p:COUNT:',
  });
  const input = {
    podUri: POD,
    selectorPatterns: [ `${POD}**/*.nq` ],
    filterFilenameTemplate: 'filters/filter-star-template-:COUNT:',
    nResources: 2,
    context: { parameterNames: [[ '$s$', '$p1$', '$o1$' ], [ '$s$', '$p1$', '$o1$', '$p2$', '$o2$' ]]},
  };

  it('refers to the filter files, counted from 1, by default', () => {
    expect(filterObjects(generator.generateMetadata(input)).map(term => term.value)).toEqual([
      `${POD}filters/filter-star-template-1`,
      `${POD}filters/filter-star-template-2`,
    ]);
  });

  it('writes the filters as literals in the order of the resources when they are given', () => {
    const quads = generator.generateMetadata({ ...input, filters: [ 'SELECT 1', 'SELECT 2' ]});
    expect(filterObjects(quads).map(term => [ term.termType, term.value ])).toEqual([
      [ 'Literal', 'SELECT 1' ],
      [ 'Literal', 'SELECT 2' ],
    ]);
    expect(quads.filter(quad => quad.predicate.value === `${NS}template`).map(quad => quad.object.value)).toEqual([
      'resources/resource-star-template-1/{s}/{p1}/{o1}',
      'resources/resource-star-template-2/{s}/{p1}/{o1}/{p2}/{o2}',
    ]);
  });
});

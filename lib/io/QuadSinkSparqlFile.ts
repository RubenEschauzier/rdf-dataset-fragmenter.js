import * as fs from 'node:fs';
import { dirname } from 'node:path';
import { clearLine, cursorTo } from 'node:readline';
import type { Writable } from 'node:stream';
import type * as RDF from '@rdfjs/types';
import { mkdirp } from 'mkdirp';
import { DataFactory } from 'rdf-data-factory';
import {
  Generator,
  Parser,
  type SparqlParser,
  type Triple,
  type ConstructQuery,
} from 'sparqljs';
import type { IQuadSink } from './IQuadSink';
import { ParallelFileWriter } from './ParallelFileWriter';

/**
 * A quad sink that writes to files using an IRI to local file system path mapping.
 */
export class QuadSinkSparqlFile implements IQuadSink {
  private readonly queryTemplate: string;
  private readonly parser: SparqlParser;
  private readonly variableReplacementIndicator: string;

  private readonly outputFormat: string;
  private readonly iriToPath: Map<RegExp, string>;
  private readonly fileWriter: ParallelFileWriter;
  protected readonly log: boolean;
  protected readonly fileExtension?: string;

  private counter = 0;
  private readonly dataFactory = new DataFactory();

  // Buffer to collect quads per IRI before writing
  private readonly quadBuffers = new Map<string, RDF.Quad[]>();

  public constructor(options: IQuadSinkSparqlFileOptions) {
    this.queryTemplate = `CONSTRUCT { } WHERE { }`;
    this.variableReplacementIndicator = options.variableReplacementIndicator;
    this.parser = new Parser();

    this.outputFormat = options.outputFormat;
    this.iriToPath = new Map(Object.entries(options.iriToPath).map(([ exp, sub ]) => [
      new RegExp(exp, 'u'),
      sub,
    ]));
    this.log = Boolean(options.log);
    this.fileExtension = options.fileExtension;
    this.fileWriter = new ParallelFileWriter({ streams: 128 });
    this.attemptLog();
  }

  protected attemptLog(newLine = false): void {
    if (this.log && (this.counter % 1_000 === 0 || newLine)) {
      clearLine(process.stdout, 0);
      cursorTo(process.stdout, 0);
      process.stdout.write(`\rHandled quads: ${this.counter / 1_000}K`);
      if (newLine) {
        process.stdout.write(`\n`);
      }
    }
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

    // Add file extension if we don't have one yet
    if (this.fileExtension && !/\.[a-z]$/iu.test(this.fileExtension)) {
      path = `${path}${this.fileExtension}`;
    }

    return path;
  }

  protected async getFileStream(path: string): Promise<Writable> {
    return this.fileWriter.getWriteStream(path, this.outputFormat);
  }

  /**
   * Fills the empty query with patterns from the quad producer.
   * @param patternQuads - Quads representing the shape (e.g. ?s foaf:name ?v1)
   */
  public buildQuery(patternQuads: RDF.Quad[]): string {
    const queryAST: ConstructQuery = <ConstructQuery> this.parser.parse(this.queryTemplate);

    // Convert quads to triples.
    const triples: Triple[] = [];

    const transformTerm = (term: RDF.Term): RDF.Term => {
      if (
        term.termType === 'NamedNode' &&
        term.value.startsWith(this.variableReplacementIndicator)
      ) {
        // Extract name: "urn:var:myVariable" -> "myVariable"
        const varName = term.value.slice(this.variableReplacementIndicator.length);
        return this.dataFactory.variable(varName);
      }
      return term;
    };

    for (const q of patternQuads) {
      triples.push({
        subject: <RDF.Quad_Subject> transformTerm(q.subject),
        predicate: <RDF.Quad_Predicate> transformTerm(q.predicate),
        object: <RDF.Quad_Object> transformTerm(q.object),
      });
    }
    queryAST.template = triples;
    queryAST.where = [
      {
        type: 'bgp',
        triples,
      },
    ];

    return new Generator().stringify(queryAST);
  }

  public async push(iri: string, quad: RDF.Quad): Promise<void> {
    this.counter++;
    this.attemptLog();

    // Buffer quads per IRI
    if (!this.quadBuffers.has(iri)) {
      this.quadBuffers.set(iri, []);
    }
    this.quadBuffers.get(iri)!.push(quad);
  }

  public async close(): Promise<void> {
    // Write all buffered queries
    for (const [ iri, quads ] of this.quadBuffers.entries()) {
      const path = this.getFilePath(iri);
      const query = this.buildQuery(quads);

      const folder = dirname(path);
      await mkdirp(folder);

      // Add query to file. Each file should have its own query
      await fs.promises.appendFile(path, `${query}`);
    }

    this.quadBuffers.clear();
    await this.fileWriter.close();
    this.attemptLog(true);
  }
}

export interface IQuadSinkSparqlFileOptions {
  /**
   * Query template to fill the quads into
   */
  queryTemplate: ConstructQuery;
  /**
   * String which indicates what should be replaced in the query template
   * by the quads passed to this sink (eg urn:var:)
   */
  variableReplacementIndicator: string;
  /**
   * The RDF format to output, expressed as mimetype.
   */
  outputFormat: string;
  /**
   * Mapping of regular expressions to their replacements,
   * for determining the file path from a given IRI.
   * @range {json}
   */
  iriToPath: Record<string, string>;
  /**
   * Whether to log quad handling progress.
   */
  log?: boolean;
  /**
   * Optional file extension to use.
   */
  fileExtension?: string;
}

export const VARIABLE_INDICATOR: RDF.NamedNode = new DataFactory().namedNode('VARIABLE_SPARQL');

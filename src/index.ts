import * as _ from 'lodash';

export interface ChunkOptions {
    transformBefore?: (collection: any[]) => any;
    transformAfterChunk?: (result: any) => any;
    transformAfterAll?: (results: any) => any;
}

/**
* chunks a collection and runs a function asynchronously on each chunk 
* @return Returns the transformed\raw result of the function for each chunk.
*/
export async function runAsAsyncChunks<T>(
    collection: any[],
    func: (input: any) => Promise<any>,
    chunkSize: number,
    parallelAsyncChunks: number,
    chunkOptions: ChunkOptions = {}): Promise<T> {
    let chunks: any[][] = _.chunk(collection, chunkSize);
    const initialChunks = chunks.splice(0, parallelAsyncChunks);

    const results: T[] = _.flatten(await Promise.all(initialChunks.map(async chunk => runChunk(chunk))));
    return _.get(chunkOptions, 'transformAfterAll') ? chunkOptions.transformAfterAll(results) : results;

    async function runChunk(chunk: any[], results: T[] = []): Promise<T[]> {
        const funcInput = _.get(chunkOptions, 'transformBefore') ? chunkOptions.transformBefore(chunk) : chunk;
        const result = await func(funcInput);
        const transformedResult = _.get(chunkOptions, 'transformAfterChunk') ? chunkOptions.transformAfterChunk(result) : result;
        results.push(transformedResult);

        const nextChunk = chunks.shift();
        nextChunk && await runChunk(nextChunk, results);

        return results;
    }
}
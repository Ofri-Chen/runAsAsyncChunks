import { ChunkData, ErrorHandlingOptions } from './index';
import * as _ from 'lodash';

export interface ChunkOptions {
    chunkSize: number;
    parallelAsyncChunks: number;

    transformBefore?: (collection: any[]) => any;
    transformAfterChunk?: (result: any) => any;
    transformAfterAll?: (results: any) => any;
    errorHandlingOptions?: ErrorHandlingOptions;
}

export interface ErrorHandlingOptions {
    retryCount?: number;
    throwError?: boolean;
    functionToRun?: (err: any, requestData: ChunkRequestData) => any;
}

export interface ChunkData {
    retryCount: number;
    chunk: any[];
}

export interface ChunkRequestData extends ChunkData {
    data: any;
}

/**
* chunks a collection and runs a function asynchronously on each chunk
* T is the result type
* @return Returns the transformed\raw result of the function for each chunk.
*/
export async function runAsAsyncChunks<T>(
    collection: any[],
    func: (input: any) => Promise<any>,
    chunkOptions: ChunkOptions): Promise<T> {

    initDefaultOptions();

    let chunks: ChunkData[] = _.chunk(collection, chunkOptions.chunkSize).map(chunk => ({ retryCount: 0, chunk }));
    const initialChunks = chunks.splice(0, chunkOptions.parallelAsyncChunks);

    const results: T[] = _.flatten(await Promise.all(initialChunks.map(async chunk => runChunk(chunk))));
    return chunkOptions.transformAfterAll(results);

    async function runChunk(chunkData: ChunkData, results: T[] = []): Promise<T[]> {
        const funcInput = chunkOptions.transformBefore(chunkData.chunk);        
        let result;
        try {
            result = await func(funcInput);
        }
        catch (err) {
            await Promise.resolve(chunkOptions.errorHandlingOptions.functionToRun(err, { ...chunkData, data: funcInput }));
            if (chunkOptions.errorHandlingOptions.retryCount > chunkData.retryCount) {
                chunkData.retryCount++;
                chunks.push(chunkData);
            } else if (chunkOptions.errorHandlingOptions.throwError) {
                throw err;
            }
        }

        if(result) {
            const transformedResult = chunkOptions.transformAfterChunk(result);
            results.push(transformedResult);
        }

        const nextChunk = chunks.shift();
        nextChunk && await runChunk(nextChunk, results);

        return results;
    }

    function initDefaultOptions() {
        chunkOptions.transformBefore = _.get(chunkOptions, 'transformBefore', (collection: any[]) => collection);
        chunkOptions.transformAfterChunk = _.get(chunkOptions, 'transformAfterChunk', ((result: any) => result));
        chunkOptions.transformAfterAll = _.get(chunkOptions, 'transformAfterAll', (results: any) => results);
        chunkOptions.errorHandlingOptions = {
            retryCount: _.get(chunkOptions, 'errorHandlingOptions.retryCount', 0),
            throwError: _.get(chunkOptions, 'errorHandlingOptions.throwError', true),
            functionToRun: _.get(chunkOptions, 'errorHandlingOptions.functionToRun', () => { })
        }
    }
}
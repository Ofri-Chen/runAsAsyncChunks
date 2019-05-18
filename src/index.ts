import { ChunkData, ErrorHandlingOptions } from './index';
import * as _ from 'lodash';
import * as uuid from 'node-uuid';

export interface ChunkResult<T> {
    result: T;
    notRetriedErrors: ChunkError[];
}

export interface ChunkError {
    chunkId: string;
    error: any;
}

export interface ChunkOptions {
    chunkSize: number;
    parallelAsyncChunks: number;

    transformBefore?: (collection: any[]) => any;
    transformAfterChunk?: (result: any) => any;
    transformAfterAll?: (results: any) => any;
    chunkIdGenerator?: () => string;
    errorHandlingOptions?: ErrorHandlingOptions;
}

export interface ErrorHandlingOptions {
    retries?: number;
    throwError?: boolean;
    functionToRun?: (err: any, requestData: ChunkRequestData) => any;
}

export interface ChunkData {
    id: string;
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
    chunkOptions: ChunkOptions): Promise<ChunkResult<T>> {

    initDefaultOptions();

    let chunks: ChunkData[] = _.chunk(collection, chunkOptions.chunkSize).map(chunk => ({
        retryCount: 0,
        chunk,
        id: chunkOptions.chunkIdGenerator()
    }));
    let notRetriedErrors: ChunkError[] = [];
    const initialChunks = chunks.splice(0, chunkOptions.parallelAsyncChunks);

    const results: T[] = _.flatten(await Promise.all(initialChunks.map(async chunk => runChunk(chunk))));
    return { result: chunkOptions.transformAfterAll(results), notRetriedErrors };

    async function runChunk(chunkData: ChunkData, results: T[] = []): Promise<T[]> {
        const funcInput = chunkOptions.transformBefore(chunkData.chunk);
        let result: any;
        try {
            result = await func(funcInput);
        }
        catch (error) {
            await Promise.resolve(chunkOptions.errorHandlingOptions.functionToRun(error, { ...chunkData, data: funcInput }));
            if (chunkOptions.errorHandlingOptions.retries > chunkData.retryCount) {
                chunkData.retryCount++;
                chunks.push(chunkData);
            } else if (chunkOptions.errorHandlingOptions.throwError) {
                throw error;
            } else {
                notRetriedErrors.push({chunkId: chunkData.id, error});
            }
        }

        if (result) {
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
        chunkOptions.chunkIdGenerator = _.get(chunkOptions, 'chunkIdGenerator', () => uuid());
        chunkOptions.errorHandlingOptions = {
            retries: _.get(chunkOptions, 'errorHandlingOptions.retries', 0),
            throwError: _.get(chunkOptions, 'errorHandlingOptions.throwError', true),
            functionToRun: _.get(chunkOptions, 'errorHandlingOptions.functionToRun', () => { })
        }
    }
}
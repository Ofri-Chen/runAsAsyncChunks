import { ChunkOptions, runAsAsyncChunks, ChunkRequestData } from ".";
import * as _ from 'lodash';

const collection = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
let counter = 0;
const chunkOpts: ChunkOptions = {
    chunkSize: 2,
    parallelAsyncChunks: 3,
    transformBefore: (chunk: number[]) => {
        console.log('running chunk:', chunk);
        return chunk.map(number => number * 2)
    },
    transformAfterChunk: (results: number[]) => {
        console.log(counter++);
        return results.map(result => result - 1)
    },
    transformAfterAll: (results: number[][]) => _.flatten(results),
    errorHandlingOptions: {
        retries: 3,
        functionToRun: (err: any, requestData: ChunkRequestData) => {
            console.log('error -', requestData.chunk);
        },
        throwError: false
    }
}

runAsAsyncChunks(collection, someAsyncFunc, chunkOpts)
    .then(console.log);

async function someAsyncFunc(numbers: number[]): Promise<number[]> {
    await wait(1000);
    // throw new Error("aaah");
    return numbers.map(num => num ** 2);
}

async function wait(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
} 
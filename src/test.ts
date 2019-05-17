import { ChunkOptions, runAsAsyncChunks } from ".";

const collection = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
let counter = 0;
const chunkOpts: ChunkOptions = {
    transformBefore: (chunk: number[]) => chunk.map(number => number * 2),
    transformAfterChunk: (results: number[]) => {
        console.log(counter++);
        return results.map(result => result - 1)
    },
    transformAfterAll: (results: number[][]) => results
}

runAsAsyncChunks(collection, someAsyncFunc, 2, 3, chunkOpts)
    .then(console.log);

async function someAsyncFunc(numbers: number[]): Promise<number[]> {
    await wait(1000);
    return numbers.map(num => num ** 2);
}

async function wait(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms));
} 
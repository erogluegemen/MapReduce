from multiprocessing import Pool


def mapper(data:list) -> list:
    results = []
    for i in data:
        # Square each number and emit key-value pairs
        results.append((i, i**2))
    return results


def reducer(data:list) -> dict.items:
    results = {}
    for k, v in data:
        # Sum all the values for each key
        if k in results:
            results[k] += v
        else:
            results[k] = v
    return results.items()


def map_reduce(data:list, num_processes:int=4) -> list:
    '''
    1) Split data into chunks
    2) Map Phase
    3) Reduce Phase
    '''

    # Split data into chunks: Parallel Processing
    chunk_size = len(data) // num_processes
    chunks = [data[i:i+chunk_size] for i in range(0, len(data), chunk_size)]

    pool = Pool(processes=num_processes)

    # Map Phase: Apply the mapper function to each chunk of data
    mapped_data = pool.map(mapper, chunks)
    flattened_data = [item for sublist in mapped_data for item in sublist]

    # Reduce Phase: Apply the reducer function to the flattened data
    reduced_data = reducer(flattened_data)

    return reduced_data


# Test the Code
if __name__ == "__main__":
    input_data = [1, 2, 3, 4, 5, 
                  6, 7, 8, 9, 10]

    result = map_reduce(input_data)

    for k, v in result:
        print(f"Key: {k}, Value: {v}")

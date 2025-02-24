### STREAMING PIPELINES ###

from typing import List, Callable


def streamingpipeline(generator, functions:List[Callable]):
    """
    Processes a generator stream through a sequence of functions
    Examples of usage:
        Real-time data processing, logs, market data
    Args:
        generator (iterable): A data stream
        functions (List[Callable]): A list of functions to apply
    Yields:
        Processed elements from the stream
    """
    for data in generator:
        for func in functions:
            data = func(data)
        yield data


if __name__ == '__main__':
    def to_uppercase(text): return text.upper()
    def add_exclamation(text): return text + '!'

    stream = iter(['hello', 'world'])
    processed_stream = streamingpipeline(stream, [to_uppercase, add_exclamation])

    for item in processed_stream:
        print(item)

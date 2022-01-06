# spark-flight

Reads flight data from csv files and calculates the top N source airports. Output is written as csv files.

## Build

    mvn clean package

    docker build -t flights .

## Run

    mkdir input output spark-checkpoints
    docker run --rm -v input:/opt/flights/input -v output:/opt/flights/output \
        -v spark-checkpoints:/opt/flights/spark-checkpoints flights

Remember that volumes work differently under WSL.

    mkdir /c/Users/.../flights/input /c/Users/.../flights/output /c/Users/.../flights/spark-checkpoints
    docker run --rm -v /c/Users/.../flights/input:/opt/flights/input \
        -v /c/Users/.../flights/output:/opt/flights/output \
        -v /c/Users/.../flights/spark-checkpoints:/opt/flights/spark-checkpoints \
        flights

## Demo

Spark can only know when to close a window when a new watermark has arrived, so it can only output the last input data after the next-to-last input data has arrived. If the source is idle, the last input data will held in the window indefinitely rather then being output.

To keep a demo running, place source data files in the input directory every few seconds. 

## Explanation of code

Windowing on a stream can only be done with time windows in spark. Therefore we need to have a notion of time in our dataset. But the dataset does not have a column that contains an event time that can be used as a time indicator. Therefore we add the current time as a processing time that we can use as a watermark.

The window size and slide duration are parameterized. The chosen defaults of 5 and 1 seconds seem reasonable for use in a demo, but can be changed if the environment demands it without having to change code.

In batch mode, the sort-by-count could be performed using the spark api. In streaming mode, sorting a window can only be done in combination with 'complete' output mode, but file sinks only support 'append'. Therefore calculating the top airports has to be done in plain scala.

Output is partitioned by window, so it will create a single top list per file. Obviously this can easily be changed.

## Unit tests

In spark 2.x testing structured streaming was done using a `MemoryStream`. This time I could not get this to work, I assume this is because in 3.1.1 things work differently. Inspection of spark's own unit tests on github suggests a more strait forward approach for unit testing streams; an approach that did not require `MemoryStream`.

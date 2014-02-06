#samza-file-reader

samza-file-reader is a Samza System that places the contents of a
specified file onto a Samza compatible stream. This stream can then be
processed by a Samza StreamTask as per any other Samza compatible stream
(such as a Kafka stream).

There is an example StreamTask configuration file included in this
project for those interested in. The file is in the repo root named:

      file-reader-example.properties

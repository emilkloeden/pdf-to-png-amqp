# PDF to PNG AMQP

A single file node application that listens on the RabbitMQ exchange called "books" for json
representations of an `event` (an object with two properties:
type: string
data: object

It expects the data object to contain a PDFPath and a PNGPath properties that denote the location of a PDF file and a folder in which to store output, respectively.

It then converts the input PDF into one PNG image per page.

It's intended purpose is to be used in the "books" project (github.com/emilkloeden/books) as
part of the book upload and conversion pipeline

By default it expects to find RabbitMQ
at localhost:5672.

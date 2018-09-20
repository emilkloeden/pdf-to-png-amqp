/**
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

*/

const fs = require("fs");
const amqp = require("amqplib/callback_api");
const makeDir = require("make-dir");
const PDFImage = require("pdf-image").PDFImage;

const amqpURL = process.env.AMQP_URL || "amqp://localhost:5672";

function makeDirIfNotExistsSync(path) {
  const directoryExists = fs.existsSync(path);
  return directoryExists ? path : makeDir.sync(path);
}

function createPNGs(ch, msg, PDFPath, PNGPath) {
  makeDirIfNotExistsSync(PNGPath);

  const convertOptions = {
    "-alpha": "Off"
  };
  const pdfImage = new PDFImage(PDFPath, convertOptions);
  pdfImage.outputDirectory = PNGPath;

  pdfImage
    .convertFile()
    .then("convert file")
    .then(ch.ack(msg));
}

amqp.connect(
  amqpURL,
  (err, conn) => {
    if (err) {
      // If we error out here, we might as well quit
      console.error(`Error in connecting to RabbitMQ: ${err}`);
      process.exit(1);
    }
    conn.createChannel((err, ch) => {
      if (err) {
        console.error(`Error creating channel: ${err}`);
      }
      var ex = "books";

      ch.assertExchange(ex, "fanout", { durable: false });
      ch.assertQueue("png", { exclusive: true }, (err, q) => {
        if (err) {
          console.error(`Error asserting queue: ${err}`);
        }
        console.log(
          ` [*] Waiting for messages in ${q.queue}. To exit press CTRL+C`
        );

        ch.bindQueue(q.queue, ex, ""); // see if third arg 'pattern' can be made use of

        ch.consume(
          q.queue,
          msg => {
            try {
              const e = JSON.parse(msg.content.toString());
              console.log(`MESSAGE: ${e.type} - ${new Date()}`);
              const { PDFPath, PNGPath } = e.data;
              createPNGs(ch, msg, PDFPath, PNGPath);
            } catch (err) {
              console.error(`Error in channel.consume: ${err}`);
              ch.nack(msg, false, false); // @TODO: confirm arg values
            }
          },
          { noAck: false }
        );
      });
    });
  }
);

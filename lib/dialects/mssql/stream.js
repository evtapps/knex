const { Readable } = require('stream');

class MsSqlQuerySteam extends Readable {
  i = 0;
  tmpRows = [];

  constructor(req, options) {
    super(Object.assign({ objectMode: true, highWaterMark: 256 }, options));

    //mssql streaming request: https://github.com/tediousjs/node-mssql#streaming
    this.req = req;
    this.req.stream = true;

    this._reading = false;
    this._closed = false;

    this.batchSize = (options || {}).batchSize || 1000;

    this.req.on('error', (err) => {
      this.emit('error', err);
      this.destroy(err);
    });

    this.req.on('done', (_) => {
      this._reading = false;
      this._closed = true;
      this.push();
    });

    this.req.on('row', (row) => {
      this._closed = false;
      this._reading = true;
      this._buffer(row);
    });
  }

  // this is implemented to deal with https://github.com/tediousjs/tedious/issues/1139
  destroy(err) {
    this.emit('close');
    return;
  }

  _buffer(obj) {
    ++this.i;

    if (this.i % this.batchSize === 0) {
      this.req.pause();
      this.push();
    }
    this.tmpRows.push(obj);
  }

  _read(size) {
    const sendBatch = Math.max(size, this.batchSize);

    if (this._closed && this._reading == false) {
      if (this.tmpRows.length) {
        const sliceArr = this.tmpRows.splice(0, sendBatch);
        sliceArr.forEach((slice) => {
          this.push(slice);
        });
      }
      setImmediate(() => this.emit('close'));
      return this.push(null);
    }

    if (this.tmpRows.length >= sendBatch || this._closed) {
      const sliceArr = this.tmpRows.splice(0, sendBatch);
      sliceArr.forEach((slice) => {
        this.push(slice);
      });
    }
    //consumed all array rows and can get more
    if (
      this.tmpRows.length == 0 &&
      this._reading == true &&
      this._closed == false
    ) {
      //trigger resume to read more data
      this.req.resume();
    }
    return;
  }
}

module.exports = MsSqlQuerySteam;

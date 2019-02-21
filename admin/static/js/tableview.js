// A TableView holds a table, with the following features:
//
// - reorderable and sortable columns
// - scrollability and lazy loading of data
// - resizable width columns
// - dynamically changing columns based on the view
// - optionally, no columns (useful for a non-tabular look at the elements)
//
// TableView operations are performed on an HTML element (a div) that holds the
// table, whose dimensions are externally determined, and a data source, whose
// responsibility is to load data, and apply sortings.




// TODO: Reach into QueryResult and request more batches.
class QueryRowSource {
    constructor(rows) {
        this.rows = [];
        // TODO: Actually make use of these...
        this.pendingAfter = null;
        this.pendingBefore = null;
        if (rows) {
            // For linear query results, we don't have keys -- objects are displayed in the order
            // that they are returned by the query.
            let i = 0;
            for (let row of rows) {
                this.rows.push({key: i, row: row});
                i++;
            }
        }
    }

    addRow(row) {
        this.rows.push({key: this.rows.length, row: row});
    }

    // Returns the set of rows in [startKey, some_end_key]
    getRowsFrom(startKey) {
        console.log("getRowsFrom", startKey);
        if (this.pendingAfter != null) {
            console.log("getRowsFrom(", startKey, ") after ", this.pendingAfter);
            return;
        }
        let ret = [];
        let e = Math.min(this.rows.length, startKey + 10);
        for (let i = startKey; i < e; i++) {
            ret.push(this.rows[i]);
        }
        // TODO: Support loading more data... or don't depend on QueryResult.
        let lastRow = e == this.rows.length;
        return Promise.resolve({rows: ret, isEnd: lastRow});
    }

    getRowsFromStart() {
        return this.getRowsFrom(0);
    }

    // Returns the set of rows in [some_start_key, endKey).
    getRowsBefore(endKey) {
        console.log("getRowsBefore", endKey);
        if (this.pendingBefore != null) {
            console.log("getRowsBefore(", endKey, ") before ", this.pendingBefore);
            return;
        }

        let ret = [];
        let startKey = Math.max(0, endKey - 10);
        for (let i = startKey; i < endKey; i++) {
            ret.push(this.rows[i]);
        }
        return Promise.resolve(ret);
    }


    cancelPendingRequests() {
        // Nothing to do yet.
    }

}

class TableViewer {
    // el should be a div.
    constructor(el, rowSource) {
        this.el = el;
        this.rowSource = rowSource;
        // Set to true when the end of the data is reached and present in the DOM.
        this.underflow = false;
        this.frontOffset = 0;

        this.queryGeneration = 0;
        this.renderedGeneration = 0;
        this.numRedraws = 0;

        while (el.firstChild) {
            el.removeChild(el.firstChild);
        }

        this.columnHeaders = null;
        this.rowScroller = document.createElement('div');
        this.rowScroller.className = 'table_viewer_scroller';
        el.appendChild(this.rowScroller);
        this.rowHolder = document.createElement('div');
        this.rowHolder.className = 'table_viewer_holder';
        this.rowScroller.appendChild(this.rowHolder);

        this.rowScroller.onscroll = event => this.redraw();

        // General structure:
        // <div "el">
        //   <div "columnHeaders"></div>
        //   <div "rowScroller"><div "rowHolder">rows...</div></div>
        // </div>
        // But columnHeaders is absent right now.  (And no rows have been loaded either.)
    }

    wipe() {
        while (this.rowHolder.firstChild) {
            this.rowHolder.removeChild(this.rowHolder.firstChild);
        }
    }

    // TODO: Rename to "update" -- this doesn't redraw per se.
    redraw() {
        console.log("TableViewer redraw", ++this.numRedraws);
        // Our job is to look at what has been rendered, what needs to be
        // rendered, and query for more information.

        if (this.rowHolder.children.length == 0) {
            if (!this.underflow) {
                console.log("rows from start");
                let generation = ++this.queryGeneration;
                this.rowSource.getRowsFromStart().then((res) => this._supplyRows(generation, res));
            }
            return;
        }

        let scrollTop = this.rowScroller.scrollTop;
        let scrollerBoundingRect = this.rowScroller.getBoundingClientRect();
        let rowsBoundingRect = this.rowHolder.getBoundingClientRect();
        let loadPreceding = rowsBoundingRect.top > scrollerBoundingRect.top;
        let loadSubsequent =
            rowsBoundingRect.bottom < scrollerBoundingRect.bottom && !this.underflow;

        // TODO: Well, we don't really use generation, do we.
        let generation = this.queryGeneration;
        if (loadPreceding) {
            console.log("loadPreceding");
            let rowsBefore = this.rowSource.getRowsBefore(this.frontOffset).then(
                rows => this._supplyRowsBefore(generation, rows));
        }
        if (loadSubsequent) {
            console.log("loadSubsequent");
            // TODO: Grotesque rowHolder.children.length
            let rowsAfter = this.rowSource.getRowsFrom(this.rowHolder.children.length - this.frontOffset).then(
                res => this._supplyRows(generation, res));
        }
    }

    appendedSource() {
        this.underflow = false;
        // this.redraw();
    }

    cleanup() {
        this.rowSource.cancelPendingRequests();
    }

    _supplyRowsBefore(generation, rows) {
        if (generation < this.renderedGeneration) {
            console.log("Ancient request from previous generation ignored");
            return;
        }
        console.log("Supplying rows before", rows);
        if (generation > this.renderedGeneration) {
            // TODO: Think this generation stuff through.
            this.wipe();
        }
        this.renderedGeneration = generation;
        let frontNode = this.rowHolder.firstChild;  // possibly null.
        for (let row of rows) {
            this.rowHolder.insertBefore(TableViewer.makeDOMRow(row), frontNode);
        }
        // We might need to load more rows.
        // TODO: Remove 1000.
        setTimeout(() => this.redraw(), 1000);
    }

    _supplyRows(generation, res) {
        let {rows, isEnd} = res;
        if (generation < this.renderedGeneration) {
            console.log("Ancient request from previous generation ignored");
            return;
        }
        console.log("Supplying rows", rows);
        if (generation > this.renderedGeneration) {
            this.wipe();
        }
        this.renderedGeneration = generation;
        // TODO: We falsely assume append-only.
        for (let row of rows) {
            this.rowHolder.appendChild(TableViewer.makeDOMRow(row));
        }
        this.underflow = isEnd;
        console.log("this.underflow = ", this.underflow);
        // We might need to load more rows.
        // TODO: Remove 1000.
        setTimeout(() => this.redraw(), 1000);
    }

    static makeDOMRow(row) {
        let rowEl = document.createElement("p");
        rowEl.appendChild(document.createTextNode(JSON.stringify(row)));
        rowEl.className = "table_viewer_row";
        return rowEl;
    }

}

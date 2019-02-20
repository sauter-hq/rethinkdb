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




// TODO: Reach into QueryRowSource and request more batches.
class QueryRowSource {
    constructor(rows) {
        this.rows = [];
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

    // -> Promise<[object]>
    getRowsBefore(key) {
        let ret = [];
        for (let i = Math.max(0, key - 10); i < key; i++) {
            ret.push(this.rows[i]);
        }
        return Promise.resolve(ret);
    }

    // -> Promise<[object]>
    getRowsAfter(key) {
        let ret = [];
        for (let i = key + 1, e = Math.min(this.rows.length, key + 11); i < e; i++) {
            ret.push(this.rows[i]);
        }
        return Promise.resolve(ret);
    }

    // -> Promise<[object]>
    getRowsFromStart() {
        let ret = [];
        for (let i = 0, e = Math.min(10, this.rows.length); i < e; i++) {
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
        // Set to true when the data returned a short result and did not
        // completely fill up the table view. E.g. the table has enough space
        // for 50 rows, but the table only has 49 rows after our scroll point.
        // This means, if we look at the elements and see extra space, we do not
        // need to re-render.
        this.underflow = false;

        while (el.firstChild) {
            el.removeChild(el.firstChild);
        }

        this.columnHeaders = null;
        this.rowScroller = document.createElement('div');
        this.rowScroller.className = 'TableViewerRowScroller';
        el.appendChild(this.rowScroller);
        this.rowHolder = document.createElement('div');
        this.rowHolder.className = 'TableViewerRowHolder';
        this.rowScroller.appendChild(this.rowHolder);

        // General structure:
        // <div "el">
        //   <div "columnHeaders"></div>
        //   <div "rowScroller"><div "rowHolder">rows...</div></div>
        // </div>
        // But columnHeaders is absent right now.  (And no rows have been loaded either.)
    }

    redraw() {
        // Our job is to look at what has been rendered, what needs to be
        // rendered, and query for more information.

        if (this.rowHolder.children.length == 0) {
            if (!this.underflow) {
                this.rowSource.getRowsFromStart().then((rows) => this._supplyRows(rows));
            }
            return;
        }

        let scrollTop = this.rowScroller.scrollTop;
        let scrollerBoundingRect = this.rowScroller.getBoundingClientRect();
        let rowsBoundingRect = this.rowHolder.getBoundingClientRect();
        let loadPreceding = innerBoundingRect.top > outerBoundingRect.top;
        let loadSubsequent =
            innerBoundingRect.bottom < outerBoundingRect.bottom && !this.underflow;

        // TODO: pass key in here.
        if (loadPreceding) {
            let rowsBefore = rowSource.getRowsBefore().then((rows) => this._supplyRows(rows));
        }
        if (loadSubsequent) {
            let rowsAfter = rowSource.getRowsAfter().then((rows) => this._supplyRows(rows));
        }
    }

    appendedSource() {
        this.underflow = false;
        this.redraw();
    }

    cleanup() {
        this.rowSource.cancelPendingRequests();
    }

    _supplyRows(rows) {
        console.log("Supplying rows", rows);
        for (let row of rows) {
            this.rowHolder.appendChild(TableViewer.makeDOMRow(row));
        }
    }
    
    static makeDOMRow(row) {
        let pre = document.createElement("pre");
        pre.appendChild(document.createTextNode(JSON.stringify(row)));
        console.log("Made dom row", pre);
        return pre;
    }

}

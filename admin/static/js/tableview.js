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





// For testing purposes...?
class StaticRowSource {
    constructor(jsonRows) {
        this.jsonRows = jsonRows;
    }

    // -> Promise<[object]>
    getRowsBefore(key) {
        let ret = [];
        for (let i = Math.max(0, key - 10); i + 0.5 < key; i++) {
            ret.push({id: i});
        }
        return Promise.resolve(ret);
    }

    // -> Promise<[object]>
    getRowsAfter(key) {
        let ret = [];
        for (let i = key + 1; i + 0.5 < key + 11; i++) {
            ret.push({id: i});
        }
        return Promise.resolve(ret);
    }

    getRowsFromStart() {
        let ret = [];
        for (let i = 0; i < 10; i++) {
            ret.push({id: i});
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
                this.rowSource.getRowsFromStart().then((rows) => this.supplyRows(rows));
            }
            return;
        }

        let scrollTop = this.rowScroller.scrollTop;
        let scrollerBoundingRect = this.rowScroller.getBoundingClientRect();
        let rowsBoundingRect = this.rowHolder.getBoundingClientRect();
        let loadPreceding = innerBoundingRect.top > outerBoundingRect.top;
        let loadSubsequent =
            innerBoundingRect.bottom < outerBoundingRect.bottom && !this.underflow;

        if (loadPreceding) {
            let rowsBefore = rowSource.getRowsBefore().then((rows) => this.supplyRows(rows));
        }
        if (loadSubsequent) {
            let rowsAfter = rowSource.getRowsAfter().then((rows) => this.supplyRows(rows));
        }
    }

    cleanup() {
        this.rowSource.cancelPendingRequests();
    }

    supplyRows(rows) {
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

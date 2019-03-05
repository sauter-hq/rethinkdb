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


class TableRowSource {
    constructor(r, driver, tableId) {
        this.r  = r;
        this.driver = driver;
        this.tableId = tableId;

        this.cachedRows = [];
        this.cachedRowsIsEnd = false;
        this.cachedRowsOffset = 0;

        // TODO: In getRowsFrom, we need to actually ensure the rows are
        // processed in the same order the promises get pulsed.  It's fragile
        // logic.  Does JS have in-order channels?
        // TODO: Only allow one request above and below at a time.  (Or appropriate strict usage.)

        this.rightCompletionCbs = [];
    }

    // Takes a ((reql_table, table_config) -> reql_query) function and returns a reql query.
    query(queryFunc) {
        let r = this.r;
        // TODO: Why is system_db used elsewhere?
        return r.db('rethinkdb').table('table_config').get(this.tableId).do(table_config =>
            queryFunc(r.db(table_config('db')).table(table_config('name')), table_config)
        );
    }

    getRowsFrom(startIndex) {
        console.log("getRowsFrom", startIndex);
        let r = this.r;
        let query_limit = 30;  // TODO: no
        let endOffset = this.cachedRowsOffset + this.cachedRows.length;
        if (startIndex < endOffset) {
            let ix = startIndex - this.cachedRowsOffset;
            // TODO: Don't return _all_ the rows, just... 30.
            return Promise.resolve(
                {rows: this.cachedRows.slice(ix, Math.min(this.cachedRows.length, ix + query_limit)),
                 isEnd: this.cachedRowsIsEnd}
            );
        }
        if (startIndex > endOffset) {
            return Promise.reject("getRowsFrom past the end (" + startIndex + ">" + endOffset + ") len = " 
                + this.cachedRows.length + ", offset = " + this.cachedRowsOffset);
        }

        // TODO: Look at all code for == and !=.
        let needsQuery = this.rightCompletionCbs.length === 0;

        let ret = new Promise((resolve, reject) => {
            this.rightCompletionCbs.push({resolve, reject});
        });

        if (needsQuery) {
            let primary_key = 'id';  // TODO: Don't hardcode this.
            let rightKey = this.cachedRows.length === 0 ? r.minval :
                this.cachedRows[this.cachedRows.length - 1][primary_key];

            console.log("Querying between", rightKey, "with limit", query_limit);
            let q = this.query((table, table_config) => 
                table.between(rightKey, r.maxval, {leftBound: 'open'})
                    .orderBy(primary_key, {index: primary_key}).limit(query_limit));

            this.driver.run_once(q, (err, results) => {
                if (err) {
                    console.log("run_once err:", err);
                    let cbs = this.rightCompletionCbs;
                    this.rightCompletionCbs = [];
                    for (let cb of cbs) {
                        cb.reject(err);
                    }
                } else {
                    console.log("results:", results);
                    let cbs = this.rightCompletionCbs;
                    this.rightCompletionCbs = [];
                    if (err) {
                        for (let cb of cbs) {
                            cb.reject(err);
                        }
                    } else {
                        for (let result of results) {
                            this.cachedRows.push(result);
                        }
                        let isEnd = results.length < query_limit;
                        this.cachedRows.isEnd = isEnd;
                        for (let cb of cbs) {
                            console.log("calling cb with results", results);
                            cb.resolve({rows: results, isEnd: isEnd});
                        }
                    }
                }
            });
        }

        return ret;
    }

    getRowsFromStart() {
        return this.getRowsFrom(0);
    }

    getRowsBefore(endIndex) {
        console.log("getRowsBefore", endIndex);
        if (this.cachedRowsOffset > 0) {
            return Promise.reject("getRowsBefore not implemented for deletion of rows yet");
        }

        if (this.cachedRows.length < endIndex) {
            // TODO: Ensure this case is still impossible in the future.
            return Promise.reject("getRowsBefore called with too-big endIndex");
        }

        let query_limit = 30;  // TODO: no
        let value = this.cachedRows.slice(Math.max(endIndex - query_limit, 0), endIndex);
        return Promise.resolve(value);
    }

    cancelPendingRequests() {
        // TODO: Make sure this is implemented.
    }
}

// TODO: Make sure to remove this.
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

    // Returns the set of rows in [startIndex, some_end_key]
    getRowsFrom(startIndex) {
        console.log("getRowsFrom", startIndex);
        if (this.pendingAfter != null) {
            console.log("getRowsFrom(", startIndex, ") after ", this.pendingAfter);
            return;
        }
        let ret = [];
        let e = Math.min(this.rows.length, startIndex + 10);
        for (let i = startIndex; i < e; i++) {
            ret.push(this.rows[i]);
        }
        // TODO: Support loading more data... or don't depend on QueryResult.
        let lastRow = e == this.rows.length;
        return Promise.resolve({rows: ret, isEnd: lastRow});
    }

    getRowsFromStart() {
        return this.getRowsFrom(0);
    }

    // Returns the set of rows in [some_start_key, endIndex).
    getRowsBefore(endIndex) {
        console.log("getRowsBefore", endIndex);
        if (this.pendingBefore != null) {
            console.log("getRowsBefore(", endIndex, ") before ", this.pendingBefore);
            return;
        }

        let ret = [];
        let startIndex = Math.max(0, endIndex - 10);
        for (let i = startIndex; i < endIndex; i++) {
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
        console.log("Constructing TableViewer with el ", el);
        this.el = el;
        this.rowSource = rowSource;
        // Set to true when the end of the data is reached and present in the DOM.
        this.underflow = false;
        this.frontOffset = 0;
        // This may duplicate a subregion of an array in the data source -- and parallels elements
        // in this.rowHolder, but the row objects are immutable shared.
        this.rows = [];
        // TODO: Simplify the data storage?
        this.waitingBelow = false;
        this.waitingAbove = false;

        this.queryGeneration = 0;
        this.renderedGeneration = 0;
        this.numRedraws = 0;

        while (el.firstChild) {
            el.removeChild(el.firstChild);
        }

        let styleNode = document.createElement('style');
        styleNode.type = "text/css";
        this.styleNode = styleNode;
        el.appendChild(styleNode);

        this.columnHeaders = null;
        this.rowScroller = document.createElement('div');
        this.rowScroller.className = TableViewer.className + ' table_viewer_scroller';
        el.appendChild(this.rowScroller);

        this.columnHeaders = document.createElement('table');
        this.columnHeaders.className = TableViewer.className + ' table_viewer_headers';
        this.rowScroller.appendChild(this.columnHeaders);

        this.rowHolder = document.createElement('table');
        this.rowHolder.className = 'table_viewer_holder';
        this.rowScroller.appendChild(this.rowHolder);

        this.rowScroller.onscroll = event => this.redraw();

        this.keys_count = TableViewer.initial_keys_count();
        // Parallel arrays: columnSpec has stuff like column widths.
        this.flatten_attr = [];
        this.columnSpec = [];

        // General structure:
        // <div "el">
        //   <div "columnHeaders"></div>
        //   <div "rowScroller"><div "rowHolder">rows...</div></div>
        // </div>
        // But columnHeaders is absent right now.  (And no rows have been loaded either.)
    }

    // TODO: Rename to "update" -- this doesn't redraw per se.
    redraw() {
        let preload_ratio = 0.5;  // TODO: What number?
        let overscroll_ratio = 1.5;  // TODO: A bigger number.
        console.log("TableViewer redraw", ++this.numRedraws);
        // Our job is to look at what has been rendered, what needs to be
        // rendered, and query for more information.

        if (this.rowHolder.children.length == 0) {
            if (!this.underflow) {
                console.log("rows from start");
                let generation = ++this.queryGeneration;
                console.log("rowSource:", this.rowSource);
                this.waitingBelow = true;
                this.rowSource.getRowsFromStart().then((res) => this._supplyRows(generation, res));
            }
            return;
        }

        let scrollTop = this.rowScroller.scrollTop;
        let scrollerBoundingRect = this.rowScroller.getBoundingClientRect();
        let rowsBoundingRect = this.rowHolder.getBoundingClientRect();
        let loadPreceding = !this.waitingAbove && rowsBoundingRect.top > scrollerBoundingRect.top - scrollerBoundingRect.height * preload_ratio;
        let loadSubsequent = !this.waitingBelow &&
            rowsBoundingRect.bottom < scrollerBoundingRect.bottom + scrollerBoundingRect.height * preload_ratio && !this.underflow;

        console.log("this.rowHolder", this.rowHolder);
        let middleIndex = Math.floor(this.rowHolder.children.length / 2);
        console.log("middleIndex", middleIndex);
        let middleBoundingRect = this.rowHolder.children[middleIndex].getBoundingClientRect();

        // TODO: Delete earlier rows too.
        if (!loadSubsequent && middleBoundingRect.top > scrollerBoundingRect.bottom + scrollerBoundingRect.height * overscroll_ratio) {
            
            console.log("Deleting rows >= index", middleIndex);
            let toDelete = this.rowHolder.children.length - middleIndex;
            this.rows.splice(this.rows.length - toDelete, toDelete);
            this.setDOMRows();
            this.underflow = false;
        } else if (!loadPreceding && middleBoundingRect.bottom < scrollerBoundingRect.top - scrollerBoundingRect.height * overscroll_ratio) {
            let scrollDistance = middleBoundingRect.top - rowsBoundingRect.top;
            console.log("Deleting rows < index", middleIndex);
            let toDelete = middleIndex;
            this.rows.splice(0, toDelete);
            this.setDOMRows();
            this.frontOffset += toDelete;
            console.log("incred frontOffset by ", toDelete, "to", this.frontOffset);
            // TODO: Set Padding/margin above elements for smooth scrolling.
        }

        // TODO: Well, we don't really use generation, do we.
        let generation = this.queryGeneration;
        if (loadPreceding) {
            console.log("loadPreceding");
            this.waitingAbove = true;
            let rowsBefore = this.rowSource.getRowsBefore(this.frontOffset).then(
                rows => this._supplyRowsBefore(generation, rows));
        }
        if (loadSubsequent) {
            console.log("loadSubsequent, length ", this.rowHolder.children.length, "frontOffset", this.frontOffset);
            this.waitingBelow = true;
            // TODO: Grotesque rowHolder.children.length
            let rowsAfter = this.rowSource.getRowsFrom(this.frontOffset + this.rowHolder.children.length).then(
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
        if (!this.waitingAbove) {
            console.log("_supplyRowsBefore while not waitingAbove");
            return;
        }
        this.waitingAbove = false;
        if (generation < this.renderedGeneration) {
            console.log("Ancient request from previous generation ignored");
            return;
        }
        console.log("Supplying rows before", rows);
        if (generation > this.renderedGeneration) {
            console.log("wiping");
            // TODO: Think this generation stuff through.  Probably have generationed waitingAbove/Below.
            this.rows = [];
        }
        this.renderedGeneration = generation;
        this.rows = rows.concat(this.rows);
        this.setDOMRows();
        this.frontOffset -= rows.length;
        console.log("decred frontOffset by ", rows.length, "to", this.frontOffset);
        // We might need to load more rows.
        // TODO: Remove 250.
        if (rows.length > 0) {
            setTimeout(() => this.redraw(), 250);
        }
    }

    _supplyRows(generation, res) {
        let {rows, isEnd} = res;
        if (!this.waitingBelow) {
            console.log("_supplyRows while not waitingBelow");
            return;
        }
        this.waitingBelow = false;
        if (generation < this.renderedGeneration) {
            console.log("Ancient request from previous generation ignored");
            return;
        }
        console.log("Supplying rows", rows);
        if (generation > this.renderedGeneration) {
            this.rows = [];
        }
        this.renderedGeneration = generation;
        // TODO: We falsely assume append-only.
        for (let row of rows) {
            this.rows.push(row);
        }
        this.setDOMRows();
        this.underflow = isEnd;
        console.log("this.underflow = ", this.underflow);
        // We might need to load more rows.
        // TODO: Remove 250.
        if (rows.length > 0) {
            setTimeout(() => this.redraw(), 250);
        }
    }

    static isPlainObject(value) {
        // TODO: Figure out the appropriate choice here.
        return Object.prototype.toString.call(value) == "[object Object]";
    }

    static build_map_keys(keys_count, result) {
        if (this.isPlainObject(result)) {
            let rt = result.$reql_type$;
            if (rt === 'TIME' || rt === 'BINARY') {
                keys_count.primitive_value_count++;
                keys_count.new_primitive_value_count++;
            } else {
                for (let key in result) {
                    let kco = keys_count.object;
                    if (kco === undefined) {
                        kco = {};
                        keys_count.object = kco;  // Only defined if there are keys!
                        keys_count.new_keys = {};
                    }
                    if (kco[key] === undefined) {
                        kco[key] = this.initial_keys_count();
                        keys_count.new_keys[key] = true;
                    }
                    this.build_map_keys(kco[key], result[key]);
                }
            }
        } else {
            keys_count.primitive_value_count++;
            keys_count.new_primitive_value_count++;
        }
    }

    // Compute occurrence of each key. The occurence can be a float since we
    // compute the average occurence of all keys for an object.
    static compute_occurrence(keys_count) {
        let kco = keys_count.object;
        if (kco === undefined) {
            // Just a primitive value.
            keys_count.occurrence = keys_count.primitive_value_count;
        } else {
            let count_key = keys_count.primitive_value_count > 0 ? 1 : 0;
            let count_occurrence = keys_count.primitive_value_count;
            for (let key in kco) {
                let row = kco[key];
                count_key++;
                this.compute_occurrence(row);
                count_occurrence += row.occurrence;
            }
            keys_count.occurrence = count_occurrence/count_key;  // count_key cannot be 0.
        }
    }

    static get primitive_key() { return '_-primitive value-_--'; } // TODO: Gross.
    static get className() { return 'tableviewer'; }

    // Sort the keys per level.
    static order_keys(keys) {
        let copy_keys = [];
        if (keys.object !== undefined) {
            for (let key in keys.object) {
                let value = keys.object[key];
                if (this.isPlainObject(value)) {
                    this.order_keys(value);
                }
                if (keys.new_keys && keys.new_keys[key]) {
                    copy_keys.push({key: key, value: value.occurrence});
                }
            }
            // If we could know if a key is a primary key, that would be awesome.
            // TODO: ^
            // TODO: Figure out and explain why the values are reverse-sorted relative to the keys.
            copy_keys.sort((a, b) =>
                b.value - a.value || (a.key > b.key ? 1 : -1));
        }
        keys.sorted_keys = copy_keys.map(d => d.key);
        if (keys.primitive_value_count > 0 && keys.primitive_value_count == keys.new_primitive_value_count) {
            keys.sorted_keys.unshift(this.primitive_key);
        }
        keys.new_keys = null;
        keys.new_primitive_value_count = 0;
    }

    // Flatten the object returns by build_map_keys().  We get back an array of keys.
    static get_all_attr(keys_count, attr, prefix, prefix_str) {
        for (let key of keys_count.sorted_keys) {
            if (key === this.primitive_key) {
                let new_prefix_str = prefix_str;
                // Pop the last dot.
                if (new_prefix_str.length > 0) {
                    new_prefix_str = new_prefix_str.slice(0, -1);
                }
                attr.push({prefix: prefix, prefix_str: new_prefix_str, is_primitive: true});
            } else {
                if (keys_count.object[key].object !== undefined) {
                    let new_prefix = prefix.slice();
                    new_prefix.push(key);
                    this.get_all_attr(keys_count.object[key], attr, new_prefix, (prefix_str || '') + key + '.');
                } else {
                    attr.push({prefix: prefix, prefix_str: prefix_str, key: key});
                }
            }
        }
    }

    static initial_keys_count() {
        return {primitive_value_count: 0, new_primitive_value_count: 0};
    }

    static flatten_attrs(keys_count, flatten_attr, rows) {
        for (let row of rows) {
            this.build_map_keys(keys_count, row);
        }
        this.compute_occurrence(keys_count);
        this.order_keys(keys_count);

        let orig_length = flatten_attr.length;
        this.get_all_attr(keys_count, flatten_attr, [], '');

        for (let i = orig_length; i < flatten_attr.length; i++) {
            flatten_attr[i].col = i;
        }
    }

    static json_to_table_get_attr(flatten_attr) {
        let tr = document.createElement('tr');
        tr.className = this.className + ' attrs';
        for (let col in flatten_attr) {
            let attr_obj = flatten_attr[col];
            console.log("Column attr: ", attr_obj);
            let el = document.createElement('td');
            el.className = 'col-' + col;
            if (attr_obj.key === undefined) {
                if (attr_obj.prefix.length > 0) {
                    let text = attr_obj.prefix.reduceRight(((acc, cur) => cur + '.' + acc));
                    el.appendChild(document.createTextNode(text + ' '));
                }
                let value = document.createElement('i');
                value.appendChild(document.createTextNode('value'));
                el.appendChild(value);
            } else {
                let text = attr_obj.prefix.reduceRight(((acc, cur) => cur + '.' + acc), attr_obj.key);
                el.appendChild(document.createTextNode(text));
            }
            tr.appendChild(el);
        }
        return tr;
    }

    static json_to_table_get_values(rows, flatten_attr) {
        console.log("json_to_table_get_values");
        let document_list = [];
        for (let i in rows) {
            let single_result = rows[i];
            let new_document = {cells: []};
            for (let col in flatten_attr) {
                let attr_obj = flatten_attr[col];
                let key = attr_obj.key;
                let value = single_result;
                for (let prefix of attr_obj.prefix) {
                    value = value && value[prefix];
                }
                if (!attr_obj.is_primitive) {
                    value = value ? value[key] : undefined;
                }
                new_document.cells.push(this.makeDOMCell(value, col));
            }
            let index = i + 1;
            this.tag_record(new_document, i + 1);
            document_list.push(new_document);
        }
        return this.helpMakeDOMRows(document_list)
    }

    static tag_record(doc, index) {
        // TODO: Use query position to tag record.
    }

    static makeDOMCell(value, col) {
        // TODO: Implement for real.
        let data = this.compute_data_for_type(value, col);
        let el = document.createElement('td');
        el.appendChild(document.createTextNode(data.value + ''));
        el.className = 'col-' + col;
        return el;
    }

    setColumnWidth(col, width) {
        console.log("setColumnWidth", col, ", ", width);
        let sheet = this.styleNode.sheet;
        console.log("setColumnWidth with sheet", sheet);
        while (sheet.cssRules.length <= col) {
            let i = sheet.cssRules.length;
            sheet.insertRule('.' + TableViewer.className + ' .col-' + i + ' { }', i);
        }
        console.log("padded out rules, deleting", col);
        sheet.deleteRule(col);
        console.log("deleted rule");
        sheet.insertRule(
            '.' + TableViewer.className + ' .col-' + col + ' { width: ' + width + 'px; }',
            col);
        console.log("inserted rule");
    }

    static helpMakeDOMRows(document_list) {
        let ret = [];
        for (let i in document_list) {
            let el = document.createElement('tr');
            el.className = this.className;
            for (let cell of document_list[i].cells) {
                el.appendChild(cell);
            }
            ret.push(el);
        }
        return ret;
    }

    static date_to_string(value) {
        return util.date_to_string(value);  // TODO: Implement.
    }

    static binary_to_string(value) {
        return util.binary_to_string(value);  // TODO: Implement.
    }

    static compute_data_for_type(value, col) {
        let data = {value: value, class_value: 'value-' + col};
        let value_type = typeof value;
        if (value === null) {
            data.value = 'null';
            data.classname = 'jta_null';
        } else if (value === undefined) {
            data.value = 'undefined';
            data.classname = 'jta_undefined';
        } else if (value.constructor && value.constructor === Array) {
            if (value.length === 0) {
                data.value = '[ ]';
                data.classname = 'empty array';
            } else {
                data.value = '[ ... ]';
                data.data_to_expand = JSON.stringify(value);
            }
        } else if (this.isPlainObject(value)) {
            if (value.$reql_type$ === 'TIME') {
                data.value = this.date_to_string(value);
                data.classname = 'jta_date';
            } else if (value.$reql_type$ === 'BINARY') {
                data.value = this.binary_to_string(value);
                data.classname = 'jta_bin';
            } else {
                data.value = '{ ... }';
                data.is_object = true;
            }
        } else if (value_type === 'number') {
            data.classname = 'jta_num';
        } else if (value_type === 'string') {
            if (/^(http:https):\/\/[^\s]+$/i.test(value)) {
                data.classname = 'jta_url';
            } else if (/^[a-z0-9]+@[a-z0-9]+.[a-z0-9]{2,4}/i.test(value)) {
                data.classname = 'jta_email';
            } else {
                data.classname = 'jta_string';
            }
        } else if (value_type === 'boolean') {
            data.classname = 'jta_bool';
            data.value = value === true ? 'true' : 'false';
        }

        return data;
    }

    setDOMRows() {
        console.log("setDOMRows");
        while (this.rowHolder.firstChild) {
            this.rowHolder.removeChild(this.rowHolder.firstChild);
        }
        while (this.columnHeaders.firstChild) {
            this.columnHeaders.removeChild(this.columnHeaders.firstChild);
        }

        TableViewer.flatten_attrs(this.keys_count, this.flatten_attr, this.rows);
        while (this.columnSpec.length < this.flatten_attr.length) {
            this.columnSpec.push({});
        }
        let trs = TableViewer.json_to_table_get_values(this.rows, this.flatten_attr);
        let attr_row = TableViewer.json_to_table_get_attr(this.flatten_attr);

        this.columnHeaders.appendChild(attr_row);

        for (let tr of trs) {
            this.rowHolder.appendChild(tr);
        }

        if (trs.length > 0) {
            let tr = trs[0];
            let i = 0;
            for (let child of tr.children) {
                let rect = child.getBoundingClientRect();
                console.log("Child ", i, "width:", rect.width);
                // TODO: -4?  Okay.  Get intellectual control of layout.
                let width = (rect.width - 4);
                this.setColumnWidth(i, width);
                i++;
            }
        }

    }


    // TODO: Remove.
    static makeDOMRow(row) {
        let rowEl = document.createElement("p");
        rowEl.appendChild(document.createTextNode(JSON.stringify(row)));
        rowEl.className = "table_viewer_row";
        return rowEl;
    }

}

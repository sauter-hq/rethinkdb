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
    constructor(r, driver, tableId, orderSpec) {
        this.r  = r;
        this.driver = driver;
        this.tableId = tableId;
        this.orderSpec = orderSpec;

        this.cachedRows = [];
        this.cachedRowsIsEnd = false;
        this.cachedRowsOffset = 0;

        // TODO: In getRowsFrom, we need to actually ensure the rows are
        // processed in the same order the promises get pulsed.  It's fragile
        // logic.  Does JS have in-order channels?
        // TODO: Only allow one request above and below at a time.  (Or appropriate strict usage.)

        this.rightCompletionCbs = [];

        this.primaryKey = 'id';  // TODO: Don't hardcode this.
    }

    // Other data sources might return null.
    primaryKeyOrNull() {
        return this.primaryKey;
    }

    // Takes a ((reql_table, table_config) -> reql_query) function and returns a reql query.
    query(queryFunc) {
        let r = this.r;
        // TODO: Why is system_db used elsewhere?
        return r.db('rethinkdb').table('table_config').get(this.tableId).do(table_config =>
            queryFunc(r.db(table_config('db')).table(table_config('name')), table_config)
        );
    }

    static unfurl(row, path) {
        let q = row;
        for (let key of path) {
            q = q(key);
        }
        return q.default(null);
    }

    static access(obj, path) {
        for (let key of path) {
            obj = obj && obj[key];
        }
        return obj;
    }

    helpSeek(value) {
        // For now give a phony answer.
        // TODO: Don't give a phony answer.
        return Promise.resolve({offset: 250, count: 2});
    }

    getRowsFrom(startIndex) {
        console.log("getRowsFrom", startIndex);
        let r = this.r;
        let query_limit = 300;  // TODO: no
        let endOffset = this.cachedRowsOffset + this.cachedRows.length;
        if (startIndex < endOffset) {
            let ix = startIndex - this.cachedRowsOffset;
            // TODO: Don't return _all_ the rows, just... 300.
            return Promise.resolve(
                {rows: this.cachedRows.slice(ix, Math.min(this.cachedRows.length, ix + query_limit)),
                 isEnd: this.cachedRowsIsEnd}
            );
        }
        if (startIndex > endOffset) {
            // TODO: No need to log when called from a seek... for the current hacky seek.
            console.log("getRowsFrom past the end (" + startIndex + ">" + endOffset + ") len = "
                + this.cachedRows.length + ", offset = " + this.cachedRowsOffset);
        }

        // TODO: Look at all code for == and !=.
        let needsQuery = this.rightCompletionCbs.length === 0;

        let ret = new Promise((resolve, reject) => {
            this.rightCompletionCbs.push({resolve, reject});
        });

        if (needsQuery) {
            let orderingKey = this.orderSpec.colPath;

            let rightKey = this.cachedRows.length === 0 ? r.minval :
                TableRowSource.access(this.cachedRows[this.cachedRows.length - 1], orderingKey);

            let realLimit = startIndex - endOffset + query_limit;

            console.log("Querying between", rightKey, "with limit", realLimit);
            let q;
            if (orderingKey.length === 1 && orderingKey[0] === this.primaryKey) {
                q = this.query((table, table_config) =>
                    table.between(rightKey, r.maxval, {leftBound: 'open'})
                        .orderBy({index: (this.orderSpec.desc ? r.desc : r.asc)(this.primaryKey)})
                        .limit(realLimit));
            } else {
                // TODO: Cache the cursor and set block size instead of filtering.
                // TODO: The keys are not unique, so we don't get a correct distinct ordering.
                // Use primary key as second column ordering.
                q = this.query((table, table_config) =>
                    table.filter(x => TableRowSource.unfurl(x, orderingKey).gt(rightKey))
                        .orderBy((this.orderSpec.desc ? r.desc : r.asc)(x => TableRowSource.unfurl(x, orderingKey)))
                        .limit(realLimit));
            }

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
                        let isEnd = results.length < realLimit;
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

        let query_limit = 300;  // TODO: no
        let value = this.cachedRows.slice(Math.max(endIndex - query_limit, 0), endIndex);
        return Promise.resolve(value);
    }

    cancelPendingRequests() {
        // TODO: Make sure this is implemented.
    }
}

class RealTableSpec {
    constructor(r, driver, tableId) {
        this.r = r;
        this.driver = driver;
        this.tableId = tableId;
    }

    primaryRowSource() {
        return new TableRowSource(this.r, this.driver, this.tableId, {
            colPath: ['id'],  // TODO: Hard-coded
            desc: false,
        });
    }

    // {colPath: [string], desc: bool}
    columnRowSource(orderSpec) {
        return new TableRowSource(this.r, this.driver, this.tableId, orderSpec);
    }
}


class TableViewer {
    // el should be a div.
    constructor(el, tableSpec) {
        console.log("Constructing TableViewer with el ", el);
        this.el = el;
        this.tableSpec = tableSpec;
        this.rowSource = tableSpec.primaryRowSource();
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

        el.onmousemove = (event) => { this.elMouseMove(event); }
        el.onmouseleave = (event) => {
            // TODO: This is broken.
            if (event.target === el) {
                this.elEndDrag(event);
            }
        };
        el.onmouseup = (event) => { this.elEndDrag(event); }

        let styleNode = document.createElement('style');
        styleNode.type = "text/css";
        this.styleNode = styleNode;
        el.appendChild(styleNode);

        this.seekNodeObj = TableViewer.createSeekNode();
        el.appendChild(this.seekNodeObj.div);
        this.seekNodeObj.form.onsubmit = (event) => {
            this.seek(this.seekNodeObj.input.value);
        };

        this.rowScroller = document.createElement('div');
        this.rowScroller.className = TableViewer.className + ' table_viewer_scroller';
        el.appendChild(this.rowScroller);

        this.columnHeaders = document.createElement('table');
        this.columnHeaders.className = TableViewer.className + ' table_viewer_headers';
        this.rowScroller.appendChild(this.columnHeaders);

        this.rowHolder = document.createElement('table');
        this.rowHolder.className = 'table_viewer_holder';
        this.rowScroller.appendChild(this.rowHolder);

        this.rowScroller.onscroll = event => this.fetchForUpdate();

        this.columnInfo = {
            // holds {columnName: string, display:, ...}
            order: [],
            // {key => recursive columnInfo object}
            structure: {},
            primitiveCount: 0,
            objectCount: 0,
            display: 'expanded',
            // null width means unspecified, a number means it is fixed.
            width: null,
        };

        this.displayedInfo = {
            displayedFrontOffset: 0,
            attrs: [],
            rowHolderTop: 0,
            dragging: null,
            initialRender: true,
            orderSpec: {colPath: ['id'] /* TODO: Hard-coded */, desc: false},
        };

        // General structure:
        // <div "el">
        //   <div "columnHeaders"></div>
        //   <div "rowScroller"><div "rowHolder">rows...</div></div>
        // </div>
        // But columnHeaders is absent right now.  (And no rows have been loaded either.)
    }

    fetchForUpdate() {
        // TODO: Use const where appropriate.
        const preload_ratio = 3;  // TODO: What number?  Adapt to latency?
        const overscroll_ratio = 4;
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

        let scrollerBoundingRect = this.rowScroller.getBoundingClientRect();
        let rowsBoundingRect = this.rowHolder.getBoundingClientRect();
        let loadPreceding = !this.waitingAbove && this.frontOffset > 0 && rowsBoundingRect.top > scrollerBoundingRect.top - scrollerBoundingRect.height * preload_ratio;
        let loadSubsequent = !this.waitingBelow &&
            rowsBoundingRect.bottom < scrollerBoundingRect.bottom + scrollerBoundingRect.height * preload_ratio && !this.underflow;

        console.log("this.rowHolder", this.rowHolder);
        let middleIndex = Math.floor(this.rowHolder.children.length / 2);
        console.log("middleIndex", middleIndex);
        let middleBoundingRect = this.rowHolder.children[middleIndex].getBoundingClientRect();

        if (!loadSubsequent && middleBoundingRect.top > scrollerBoundingRect.bottom + scrollerBoundingRect.height * overscroll_ratio) {

            console.log("Deleting rows >= index", middleIndex);
            let toDelete = this.rowHolder.children.length - middleIndex;
            this.rows.splice(this.rows.length - toDelete, toDelete);
            this.setDOMRows(0);
            this.underflow = false;
        } else if (!loadPreceding && middleBoundingRect.bottom < scrollerBoundingRect.top - scrollerBoundingRect.height * overscroll_ratio) {
            let scrollDistance = middleBoundingRect.top - rowsBoundingRect.top;
            console.log("Deleting rows < index", middleIndex);
            let toDelete = middleIndex;
            this.rows.splice(0, toDelete);
            this.frontOffset += toDelete;
            this.setDOMRows(scrollDistance);
            console.log("incred frontOffset by ", toDelete, "to", this.frontOffset);
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
            // TODO: We need some kind of loading bar when we get to the bottom.
            console.log("loadSubsequent, length ", this.rowHolder.children.length, "frontOffset", this.frontOffset);
            this.waitingBelow = true;
            // TODO: Grotesque rowHolder.children.length
            let rowsAfter = this.rowSource.getRowsFrom(this.frontOffset + this.rowHolder.children.length).then(
                res => this._supplyRows(generation, res));
        }
    }

    static createSeekNode() {
        let div = document.createElement('div');
        div.className = TableViewer.className + ' upper_forms';
        let form = document.createElement('form');
        form.appendChild(document.createTextNode('Seek: '));
        let input = document.createElement('input');
        input.setAttribute('type', 'text');
        input.setAttribute('placeholder', 'key');
        input.className = document.createElement('seek-box');
        form.appendChild(input);
        div.appendChild(form);
        return {div, form, input};
    }

    seek(value) {
        let parsed;
        let success = false;
        try {
            // TODO: Handle SyntaxError.
            parsed = JSON.parse(value);
            success = true;
        } catch (error) {
            // TODO: Show in UI by some means.
            if (error instanceof SyntaxError) {
                console.log("SyntaxError");
            } else {
                throw error;
            }
        }
        if (success) {
            // TODO: Generations, etc.
            this.rowSource.helpSeek(parsed).then((res) => {
                let {offset, count} = res;
                this.waitingBelow = true;
                this.rows = [];
                this.frontOffset = offset;
                let generation = ++this.queryGeneration;
                this.rowSource.getRowsFrom(offset).then(rows => {
                    this._supplyRows(generation, rows, {seek: offset, seekCount: count});
                });
                console.log("Seek not implemented: ", res);
            });
        }
    }

    appendedSource() {
        this.underflow = false;
        // this.fetchForUpdate();
    }

    cleanup() {
        this.rowSource.cancelPendingRequests();
        while (this.el.firstChild) {
            this.el.removeChild(this.el.firstChild);
        }
    }

    _supplyRowsBefore(generation, rows) {
        if (!this.waitingAbove) {
            console.log("_supplyRowsBefore while not waitingAbove");
            return;
        }
        if (generation < this.renderedGeneration) {
            console.log("Ancient request from previous generation ignored");
            return;
        }
        this.waitingAbove = false;
        console.log("Supplying rows before", rows);
        if (generation > this.renderedGeneration) {
            console.log("wiping");
            // TODO: Think this generation stuff through.  Probably have generationed waitingAbove/Below.
            this.rows = [];
        }
        this.renderedGeneration = generation;
        this.rows = rows.concat(this.rows);
        this.frontOffset -= rows.length;
        this.setDOMRows(0);
        console.log("decred frontOffset by ", rows.length, "to", this.frontOffset);
        // We might need to load more rows.
        if (rows.length > 0) {
            setTimeout(() => this.fetchForUpdate());
        }
    }

    _supplyRows(generation, res, opts) {
        let {rows, isEnd} = res;
        if (!this.waitingBelow) {
            console.log("_supplyRows while not waitingBelow");
            return;
        }
        if (generation < this.renderedGeneration) {
            console.log("Ancient request from previous generation ignored");
            return;
        }
        this.waitingBelow = false;
        console.log("Supplying rows", rows);
        if (generation > this.renderedGeneration) {
            console.log("wiping ii");
            this.rows = [];
        }
        this.renderedGeneration = generation;
        // TODO: We falsely assume append-only.
        for (let row of rows) {
            this.rows.push(row);
        }
        this.setDOMRows(0, !opts ? undefined : {seek: opts.seek, seekCount: opts.seekCount});
        this.underflow = isEnd;
        console.log("this.underflow = ", this.underflow);
        // We might need to load more rows.
        if (rows.length > 0) {
            setTimeout(() => this.fetchForUpdate());
        }
    }

    static isPlainObject(value) {
        // TODO: Figure out the appropriate choice here.
        return Object.prototype.toString.call(value) == "[object Object]";
    }

    static get className() { return 'tableviewer'; }

    elMouseMove(event) {
        let dragging = this.displayedInfo.dragging;
        if (dragging === null) {
            return;
        }
        if (event.buttons !== 1) {
            console.log("mousemove without buttons");
            this.doEndDrag();
            return;
        }
        let displacement = event.clientX - dragging.initialClientX;
        let columnInfo = this.displayedInfo.attrs[dragging.column].columnInfo;
        const min_width = 20;  // TODO: Decide on constant.
        let oldWidth = columnInfo.width;
        let newWidth = Math.max(min_width, dragging.initialWidth + displacement);
        columnInfo.width = newWidth;
        if (oldWidth !== columnInfo.width) {
            this.setColumnWidth(dragging.column, newWidth)
        }
    }

    elEndDrag(event) {
        if (this.displayedInfo.dragging === null) {
            return;
        }
        console.log("Ending drag with event ", event);
        this.doEndDrag();
    }

    doEndDrag() {
        this.displayedInfo.dragging = null;
    }

    static equalPrefix(p, q) {
        if (p.length !== q.length) {
            return false;
        }
        for (let i = 0; i < p.length; i++) {
            if (p[i] !== q[i]) {
                return false;
            }
        }
        return true;
    }

    json_to_table_get_attr(flatten_attr) {
        let tr = document.createElement('tr');
        tr.className = TableViewer.className + ' attrs';
        for (let col in flatten_attr) {
            let attr_obj = flatten_attr[col];
            console.log("Column attr: ", attr_obj);
            let tdEl = document.createElement('td');
            tdEl.className = 'col-' + col;

            // TODO: Return value on every onclick.

            let colId = col;
            tdEl.onmousedown = (event) => {
                if (event.target !== tdEl) {
                    // This is for mouse-downs in the resizing area.
                    return;
                }
                console.log("Got onclick event exclusively for tdEl #", colId);
                this.displayedInfo.dragging = {
                    column: colId,
                    initialClientX: event.clientX,
                    initialWidth: this.displayedInfo.attrs[colId].columnInfo.width || 100,  // TODO: define constant
                };
                return false;
            };
            tdEl.ondblclick = (event) => {
                if (event.target === tdEl) {
                    // This is for double-clicks outside the resizing area.
                    return;
                }
                console.log("Double clicked", colId);

                this.doEndDrag();  // TODO: idk

                let prefix = this.displayedInfo.attrs[colId].prefix;
                if (TableViewer.equalPrefix(prefix, this.displayedInfo.orderSpec.colPath)) {
                    this.displayedInfo.orderSpec = {
                        colPath: this.displayedInfo.orderSpec.colPath,
                        desc: !this.displayedInfo.orderSpec.desc,
                    };
                } else {
                    this.displayedInfo.orderSpec = {colPath: prefix, desc: false};
                }

                this.displayedInfo.initialRender = true;
                this.displayedInfo.rowHolderTop = 0;
                while (this.rowHolder.firstChild) {
                    this.rowHolder.removeChild(this.rowHolder.firstChild);
                }
                this.frontOffset = 0;
                this.rows = [];
                this.underflow = false;
                // TODO: We might have pending supplyRows actions -- we need to use generation
                // number to ignore them.
                this.waitingBelow = false;
                this.waitingAbove = false;
                this.rowHolder.style.top = '0';
                this.rowSource = this.tableSpec.columnRowSource(this.displayedInfo.orderSpec);
                this.rowScroller.scrollTo(this.rowScroller.scrollLeft, 0);
                this.fetchForUpdate();
            };

            let el = document.createElement('div');
            el.className = 'value';
            tdEl.appendChild(el);

            if (attr_obj.columnInfo.objectCount > 0) {
                if (attr_obj.columnInfo.display === 'collapsed') {
                    let arrowNode = document.createElement('div');
                    arrowNode.appendChild(document.createTextNode(' >'));
                    arrowNode.className = 'expand';
                    arrowNode.onclick = (event) => {
                        attr_obj.columnInfo.display = 'expanded';
                        this.setDOMRows(0);
                    };
                    el.appendChild(arrowNode);
                } else if (attr_obj.columnInfo.display === 'expanded') {
                    let arrowNode = document.createElement('div');
                    arrowNode.appendChild(document.createTextNode(' <'));
                    arrowNode.className = 'collapse';
                    arrowNode.onclick = (event) => {
                        attr_obj.columnInfo.display = 'collapsed';
                        this.setDOMRows(0);
                    };
                    el.appendChild(arrowNode);
                }
            }

            let text = attr_obj.prefix_str;
            el.appendChild(document.createTextNode(text));


            tr.appendChild(tdEl);
        }
        return tr;
    }

    static json_to_table_get_values(rows, frontOffset, flatten_attr) {
        console.log("json_to_table_get_values");
        let document_list = [];
        for (let i = 0; i < rows.length; i++) {
            let single_result = rows[i];
            let new_document = {cells: []};
            for (let col = 0; col < flatten_attr.length; col++) {
                let attr_obj = flatten_attr[col];
                let value = single_result;
                for (let key of attr_obj.prefix) {
                    value = value && value[key];
                }
                new_document.cells.push(this.makeDOMCell(value, col));
            }
            new_document.tag = frontOffset + i + 1;
            document_list.push(new_document);
        }
        return this.helpMakeDOMRows(document_list)
    }

    static makeDOMCell(value, col) {
        // TODO: Implement for real.
        let data = this.compute_data_for_type(value, col);
        let el = document.createElement('td');
        let inner = document.createElement('span');
        el.appendChild(inner);
        inner.appendChild(document.createTextNode(data.value + ''));
        let className = 'col-' + col + ' ' + data.classname;
        el.className = className;
        inner.className = 'col-' + col;
        return el;
    }

    setColumnWidth(col, width) {
        console.log("setColumnWidth", col, ", ", width);
        let sheet = this.styleNode.sheet;
        while (sheet.cssRules.length <= col) {
            let i = sheet.cssRules.length;
            sheet.insertRule('.' + TableViewer.className + ' td.col-' + i + ' { }', i);
        }
        sheet.deleteRule(col);
        sheet.insertRule(
            '.' + TableViewer.className + ' td.col-' + col + ' { width: ' + width + 'px; max-width: ' + width + 'px; }',
            col);
    }

    static helpMakeDOMRows(document_list) {
        let ret = [];
        for (let i = 0; i < document_list.length; i++) {
            let doc = document_list[i];
            let el = document.createElement('tr');
            let even = (doc.tag & 1) === 0;
            el.className = this.className + (even ? ' even' : ' odd');
            el.dataset.row = doc.tag;
            for (let cell of doc.cells) {
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

    /*
        this.columnInfo = {
            // holds {columnName: string, width: number}
            order: [],
            // {key => recursive columnInfo object}
            structure: {},
            primitiveCount: 0,
            objectCount: 0
        }; */


    static computeOntoColumnInfo(info, row) {
        if (this.isPlainObject(row)) {
            info.objectCount++;
            for (let key in row) {
                let obj = info.structure[key];
                if (obj === undefined) {
                    obj = this.makeNewInfo();
                    info.structure[key] = obj;
                }
                this.computeOntoColumnInfo(obj, row[key]);
            }
        } else {
            info.primitiveCount++;
        }
    }

    static makeNewInfo() {
        return {
            structure: {},
            primitiveCount: 0,
            objectCount: 0
        };
    }

    static makeColumnInfo() {
        let ret = this.makeNewInfo();
        ret.order = [];
        ret.display = 'collapsed';
        ret.width = null;
        return ret;
    }

    static orderColumnInfo(pkeyOrNull, info) {
        let keys = [];
        for (let key in info.structure) {
            this.orderColumnInfo(null, info.structure[key]);
            keys.push({
                key: key,
                count: info.structure[key].objectCount + info.structure[key].primitiveCount
            });
        }
        if (pkeyOrNull !== null) {
            keys.sort((a, b) => {
                let diff = b.count - a.count;
                if (diff !== 0) { return diff; }
                if (a.key === pkeyOrNull) {
                    return b.key === pkeyOrNull ? 0 : -1;
                }
                if (b.key === pkeyOrNull) {
                    return 1;
                }
                return a.key < b.key ? -1 : a.key > b.key ? 1 : 0;
            });
        } else {
            keys.sort((a, b) => b.count - a.count || (a.key < b.key ? -1 : a.key > b.key ? 1 : 0));
        }
        info.sorted = keys;
    }

    static computeNewColumnInfo(pkeyOrNull, rows) {
        let info = this.makeNewInfo();
        for (let row of rows) {
            this.computeOntoColumnInfo(info, row);
        }

        this.orderColumnInfo(pkeyOrNull, info);
        return info;
    }

    static mergeColumnInfo(columnInfo, newInfo) {
        columnInfo.primitiveCount += newInfo.primitiveCount;
        columnInfo.objectCount += newInfo.objectCount;
        for (let item of newInfo.sorted) {
            let key = item.key;
            let obj = columnInfo.structure[key];
            if (obj === undefined) {
                obj = this.makeColumnInfo();
                columnInfo.structure[key] = obj;
                columnInfo.order.push({columnName: key});
            }
            this.mergeColumnInfo(obj, newInfo.structure[key]);
        }
    }

    static updateColumnInfo(pkeyOrNull, columnInfo, rows) {
        let newInfo = this.computeNewColumnInfo(pkeyOrNull, rows);
        this.mergeColumnInfo(columnInfo, newInfo);
    }

    static helpEmitColumnInfoAttrs(prefix, onto, columnInfo) {
        if (prefix.length > 0 || columnInfo.primitiveCount > 0) {
            let obj = {prefix: prefix.slice(), prefix_str: prefix.join('.'), columnInfo: columnInfo};
            onto.push(obj);
        }

        if (columnInfo.display === 'collapsed') {
            return;
        }

        for (let orderEntry of columnInfo.order) {
            let key = orderEntry.columnName;
            prefix.push(key);
            this.helpEmitColumnInfoAttrs(prefix, onto, columnInfo.structure[key]);
            prefix.pop();
        }
    }

    static emitColumnInfoAttrs(columnInfo) {
        let prefix = [];
        let onto = [];
        this.helpEmitColumnInfoAttrs(prefix, onto, columnInfo);
        return onto;
    }

    applyNewAttrs(attrs) {
        let old_attrs = this.displayedInfo.attrs;
        let changed = false;
        if (old_attrs.length !== attrs.length) {
            changed = true;
        } else {
            for (let i = 0; i < old_attrs.length; i++) {
                if (old_attrs[i].prefix_str !== attrs[i].prefix_str) {
                    changed = true;
                    break;
                }
            }
        }

        this.displayedInfo.attrs = attrs;
        let attrs_row = this.json_to_table_get_attr(attrs);

        while (this.columnHeaders.firstChild) {
            this.columnHeaders.removeChild(this.columnHeaders.firstChild);
        }
        this.columnHeaders.appendChild(attrs_row);
        return changed;
    }

    setDOMRows(scrollDistance, opts) {
        console.log("setDOMRows");

        // TODO: We can just pass in unseen rows.
        TableViewer.updateColumnInfo(this.rowSource.primaryKeyOrNull(), this.columnInfo, this.rows);
        let attrs = TableViewer.emitColumnInfoAttrs(this.columnInfo);

        const attrsChanged = this.applyNewAttrs(attrs);


        const dfo = this.displayedInfo.displayedFrontOffset;
        const origLength = this.rowHolder.children.length;
        const backOffset = this.frontOffset + this.rows.length;
        const origBackOffset = dfo + origLength;


        // Find an existing row that is displayed and will continue to be displayed.
        let observedRowOffset = null;
        let observedPosition = null;
        {
            const commonFO = Math.max(this.frontOffset, dfo);
            const commonBO = Math.min(backOffset, origBackOffset);
            if (commonFO < commonBO) {
                observedRowOffset = commonFO;
                let tr = this.rowHolder.children[observedRowOffset - dfo];
                observedPosition = tr.getBoundingClientRect().top;
            }
        }


        let topAdjustment = 0;
        let insertionElement = null;
        let hasInsertionElement = false;

        if (attrsChanged) {
            // Just reconstruct all rows.
            let trs = TableViewer.json_to_table_get_values(this.rows, this.frontOffset, attrs);

            while (this.rowHolder.firstChild) {
                this.rowHolder.removeChild(this.rowHolder.firstChild);
            }
            for (let tr of trs) {
                this.rowHolder.appendChild(tr);
            }
        } else {

            // Columns are the same, see if we can incrementally update rows.
            if (this.frontOffset < dfo) {
                // We have rows in front to add.
                let trs = TableViewer.json_to_table_get_values(this.rows.slice(0, dfo - this.frontOffset),
                    this.frontOffset, attrs);
                let insertionPoint = this.rowHolder.firstChild;
                for (let tr of trs) {
                    this.rowHolder.insertBefore(tr, insertionPoint);
                    // So insertionElement gets set to the last tr.
                    insertionElement = tr;
                    hasInsertionElement = true;
                }
                console.log("Front-inserted ", trs.length, "rows");
            } else {
                // We have >= 0 rows in front to delete.
                let toDelete = Math.min(this.frontOffset - dfo, origLength);
                for (let i = 0; i < toDelete; i++) {
                    this.rowHolder.removeChild(this.rowHolder.firstChild);
                }

                topAdjustment = scrollDistance;
            }

            if (backOffset <= origBackOffset) {
                // We have rows on the end to delete.
                let toDelete = Math.min(this.rowHolder.children.length, origBackOffset - backOffset);
                for (let i = 0; i < toDelete; i++) {
                    this.rowHolder.removeChild(this.rowHolder.lastChild);
                }
            } else {
                // We have rows on the end to add.
                let toAdd = backOffset - origBackOffset;
                let trs = TableViewer.json_to_table_get_values(this.rows.slice(-toAdd), backOffset - toAdd, attrs);
                for (let tr of trs) {
                    this.rowHolder.appendChild(tr);
                }
            }
        }
        this.displayedInfo.displayedFrontOffset = this.frontOffset;

        if (this.rowHolder.firstChild) {
            let tr = this.rowHolder.firstChild;
            // We have one unaccounted for border pixel.  (Total width is 1 +
            // sum of (column widths + 5).)
            let sumWidth = 1;
            for (let i = 0; i < tr.children.length; i++) {
                let child = tr.children[i];
                let attr = this.displayedInfo.attrs[i];
                // Check if we've already specified a width -- if not, set it naturally based on the data.
                let width = attr.columnInfo.width;
                console.log("Column ", i, "width", width);
                if (width === null) {
                    let rect = child.getBoundingClientRect();
                    console.log("Child ", i, "width:", rect.width);
                    // TODO: Don't do border calculations -- don't re-specify column width
                    // in terms of its own value, either, you get column creep.
                    // With border-collapse I guess we have 1 border pixel.  And 2 px padding on both sides.
                    // 1 + 2 + 2 = 5.
                    width = rect.width - 5;
                    const min_width = 100;
                    width = Math.max(min_width, width);
                    attr.columnInfo.width = width;
                }
                this.setColumnWidth(i, width);
            }
            this.rowHolder.style.width = sumWidth + "px";
            this.columnHeaders.style.width = sumWidth + "px";
        }

        if (hasInsertionElement) {
            let rowsBoundingRect = this.rowHolder.getBoundingClientRect();
            let insertedBoundingRect = insertionElement.getBoundingClientRect();
            // This + 1 is from really precise (and fragile) border-collapse math.
            topAdjustment = rowsBoundingRect.top - insertedBoundingRect.bottom + 1;
            console.log("Inserted front, topAdjustment", topAdjustment,
                "=", rowsBoundingRect, "-", insertedBoundingRect);
        }

        if (topAdjustment !== 0) {
            let top = this.displayedInfo.rowHolderTop;
            top += topAdjustment;
            if (top < 0 || this.frontOffset === 0) {
                // Possible if the data or row heights changes.
                console.log("Top clamped:", top);
                top = 0;
            }
            this.displayedInfo.rowHolderTop = top;
            this.rowHolder.style.top = top + 'px';
        }


        let finalAdjustment;
        if (opts) {
            // On the first rerender after seeking, put the seeked-to element at the top.
            const ix = opts.seek - this.frontOffset;
            if (ix < this.rowHolder.children.length) {
                const breathingRoom = 5;
                if (this.displayedInfo.rowHolderTop < breathingRoom) {
                    this.displayedInfo.rowHolderTop = breathingRoom;
                    this.rowHolder.style.top = breathingRoom + 'px';
                }

                const elt = this.rowHolder.children[ix];
                finalAdjustment = elt.getBoundingClientRect().top - this.columnHeaders.getBoundingClientRect().bottom;
                console.log("Adjusting final adjustment with opts", opts, "and adj", finalAdjustment);
                if (opts.seek !== 0) {
                    // Show the previous row a bit, so the user knows they're at the upper bound of
                    // the matching rows via the highlighting.
                    finalAdjustment -= breathingRoom;
                }

                // Also, highlight the elements.
                // TODO: Unhighlight previously highlighted elements.
                // (This is no good -- we want the elements to stay highlighted even after removed
                // and reloaded -- the display state needs seeked state.)
                for (let i = 0; i < opts.seekCount; i++) {
                    let j = opts.seek + i - this.frontOffset;
                    if (j >= this.rowHolder.children.length) {
                        break;
                    }
                    this.rowHolder.children[j].className += ' seeked';
                }

            } else {
                finalAdjustment = 0;
            }
        } else if (observedRowOffset !== null) {
            const elt = this.rowHolder.children[observedRowOffset - this.frontOffset];
            finalAdjustment = elt.getBoundingClientRect().top - observedPosition;
        } else if (this.rowHolder.children.length > 0 && !this.displayedInfo.initialRender) {
            // TODO: Ensure empty children case is handled appropriately.
            const elt = this.rowHolder.firstChild;
            finalAdjustment = elt.getBoundingClientRect().top - this.rowScroller.getBoundingClientRect().top;
        }

        this.rowScroller.scrollBy(0, finalAdjustment);
        this.displayedInfo.initialRender = false;
    }


    // TODO: Remove.
    static makeDOMRow(row) {
        let rowEl = document.createElement("p");
        rowEl.appendChild(document.createTextNode(JSON.stringify(row)));
        rowEl.className = "table_viewer_row";
        return rowEl;
    }

}

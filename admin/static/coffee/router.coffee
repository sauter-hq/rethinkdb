# Copyright 2010-2012 RethinkDB, all rights reserved.
dashboard = require('./dashboard/dashboard.coffee')
log_view = require('./log_view.coffee')
dataexplorer_view = require('./dataexplorer.coffee')
tables_view = require('./tables/index.coffee')
table_view = require('./tables/table.coffee')
servers_index = require('./servers/index.coffee')
server_view = require('./servers/server.coffee')
app = require('./app.coffee')

class BackboneCluster extends Backbone.Router
    # Routes can end with a slash too - This is useful is the user
    # tries to manually edit the route
    routes:
        '': 'dashboard'
        'tables': 'index_tables'
        'tables/': 'index_tables'
        'tables/:id': 'table'
        'tables/:id/': 'table'
        'servers': 'index_servers'
        'servers/': 'index_servers'
        'servers/:id': 'server'
        'servers/:id/': 'server'
        'dashboard': 'dashboard'
        'dashboard/': 'dashboard'
        'logs': 'logs'
        'logs/': 'logs'
        'dataexplorer': 'dataexplorer'
        'dataexplorer/': 'dataexplorer'
        'dataexplorer/:db/:table': 'dataexplorer_table'
        'dataexplorer/:db/table/': 'dataexplorer_table'

    initialize: (data) ->
        super
        @navbar = data.navbar

        @container = $('#cluster')
        @current_view = new Backbone.View

        @bind 'route', @update_active_tab

    # Highlight the link of the current view
    update_active_tab: (route) =>
        @navbar.set_active_tab route

    index_tables: ->
        @current_view.remove()
        @current_view = new tables_view.DatabasesContainer
        @container.html @current_view.render().$el

    index_servers: (data) ->
        @current_view.remove()
        @current_view = new servers_index.View
        @container.html @current_view.render().$el

    dashboard: ->
        @current_view.remove()
        @current_view = new dashboard.View
        @container.html @current_view.render().$el

    logs: ->
        @current_view.remove()
        @current_view = new log_view.LogsContainer
        @container.html @current_view.render().$el

    dataexplorer: ->
        @current_view.remove()
        @current_view = new dataexplorer_view.Container
            state: dataexplorer_view.dataexplorer_state
        @container.html @current_view.render().$el
        @current_view.init_after_dom_rendered() # Need to be called once the view is in the DOM tree
        @current_view.results_view_wrapper.set_scrollbar() # In case we check the data explorer, leave and come back

    dataexplorer_table: (db, table) ->
        @navigate("#dataexplorer", {replace: true})
        @dataexplorer()
        @current_view.load_query_text('r.db(\'' + db + '\').table(\'' + table + '\')')
        @current_view.execute_query()

    # TODO: Maybe these functions don't belong in the router.
    goto_dataexplorer_table: (db, table) ->
        @update_active_tab('dataexplorer')
        @navigate("#dataexplorer", {replace: false})
        @dataexplorer()
        @current_view.load_query_text('r.db(\'' + db + '\').table(\'' + table + '\')')
        @current_view.execute_query()

    goto_dataexplorer_table_id: (db, table, document_id) ->
        @update_active_tab('dataexplorer')
        @navigate("#dataexplorer", {replace: false})
        @dataexplorer()
        @current_view.load_query_text('r.db(\'' + db + '\').table(\'' + table + '\').get(' + document_id + ')')
        @current_view.execute_query()

    table: (id) ->
        @current_view.remove()
        @current_view = new table_view.TableContainer id
        @container.html @current_view.render().$el

    server: (id) ->
        @current_view.remove()
        @current_view = new server_view.View(id)
        @container.html @current_view.render().$el


exports.BackboneCluster = BackboneCluster

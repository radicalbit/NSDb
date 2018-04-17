# NSDB

## Installation

IMPORTANT: create these files under the project root, with the same content

.env.development.local

.env.test.local

.env.production.local

```txt
REACT_APP_NSDB_HTTP=http://localhost:4000
REACT_APP_NSDB_WS=ws://localhost:4040
```

## Available scripts

```txt
    "precommit": automatically triggered before each commit,
    "http": start the http mock server,
    "ws": start the ws mock server,
    "mock": start both mock servers,
    "start": start the app in dev mode,
    "start:mock": start the app in dev mode with mock servers,
    "test": execute tests,
    "test:mock": execute tests using mock servers,
    "build": create the prod build,
    "setup": install dependencies and create prod build
```

## Project structure

/config: webpack configuration

/public: basic html template

/scripts: react scripts for dev, test and prod

/server:

* /mock: http and ws mock servers

/src:

* /api: http and ws endpoints
* /assets: fonts, theme and palette
* /components: reusable components which are not connected to Redux
* /containers: application specific components connected to Redux
* /pages: main application routes
* /store: Redux store, containing reducers and epics
* /utils: various helpers
* configureStore.js: Redux entrypoint
* index.js: App entrypoint

## React ecosystem

This projects uses the following libraries in addition to React/Redux:

* [react-router](https://github.com/ReactTraining/react-router): de-facto routing library for react
* [react-loadable](https://github.com/jamiebuilds/react-loadable): used for code splitting and lazy loading
* [react-form](https://github.com/react-tools/react-form): used in login and query builder
* [react-table](https://github.com/react-tools/react-table): used to handle query results
* [reselect](https://github.com/reactjs/reselect): de-facto selector library
* [re-reselect](https://github.com/toomuchdesign/re-reselect): used to solve reselect problem on multiple selectors
* [redux-observable](https://github.com/redux-observable/redux-observable): async middleware based on RxJS

## Todo List

Implement authentication, which involves the following steps:

1. Create a loginService under /api folder.
2. Create types, actions, reducers and epics under the /store/auth folder.
3. Modify form validation rules under /utils/validations.js

Add headers to http requests:

1. After having implemented authentication, we need a way to add headers to request based on auth status.
2. Use getState() inside the /utils/request.js or remove the fetch api in favor or Observable api (preferred way).
3. Add CORS support by modifying /utils/cors.js.

Error handling:

1. Add notifications in order to make the user aware of errors.
2. Handle websocket connection/reconnection (have a look at RxJS retry method or use a RxJS wrapper).

Improve environments:

1. Change the webpack config to use an easier environment system. Actually, the .local files takes precedence.

Add Insert and Delete queries:

1. This is not too hard, because inserts and deletes are request/response.

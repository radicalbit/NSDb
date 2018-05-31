import { combineEpics } from 'redux-observable';
import { Observable } from 'rxjs';
import { types as TYPES } from './types';
import { actions } from './actions';
import {
  fetchDatabases,
  fetchNamespaces,
  fetchMetrics,
  fetchMetricDescription,
} from '../../../api/nsdbService';

const fetchDatabasesEpic = action$ =>
  action$.ofType(TYPES.DATABASES_FETCH_REQUEST).switchMap(({ payload }) =>
    Observable.fromPromise(fetchDatabases())
      .map(response => actions.fetchDatabasesSuccess(response))
      .catch(error =>
        Observable.of(actions.fetchDatabasesError(error.xhr ? error.xhr.response : error))
      )
  );

const fetchNamespacesEpic = action$ =>
  action$.ofType(TYPES.NAMESPACES_FETCH_REQUEST).switchMap(({ payload }) =>
    Observable.fromPromise(fetchNamespaces(payload.database))
      .map(response => actions.fetchNamespacesSuccess(payload.database, response))
      .catch(error =>
        Observable.of(actions.fetchNamespacesError(error.xhr ? error.xhr.response : error))
      )
  );

const fetchMetricsEpic = action$ =>
  action$.ofType(TYPES.METRICS_FETCH_REQUEST).switchMap(({ payload }) =>
    Observable.fromPromise(fetchMetrics(payload.database, payload.namespace))
      .map(response => actions.fetchMetricsSuccess(payload.namespace, response))
      .catch(error =>
        Observable.of(actions.fetchMetricsError(error.xhr ? error.xhr.response : error))
      )
  );

const fetchMetriDescriptionEpic = action$ =>
  action$.ofType(TYPES.METRIC_DESCRIPTION_FETCH_REQUEST).switchMap(({ payload }) =>
    Observable.fromPromise(
      fetchMetricDescription(payload.database, payload.namespace, payload.metric)
    )
      .map(response =>
        actions.fetchMetricDescriptionSuccess(payload.namespace, payload.metric, response)
      )
      .catch(error =>
        Observable.of(actions.fetchMetricDescriptionError(error.xhr ? error.xhr.response : error))
      )
  );

const editorEpic = combineEpics(
  fetchDatabasesEpic,
  fetchNamespacesEpic,
  fetchMetricsEpic,
  fetchMetriDescriptionEpic
);

export default editorEpic;

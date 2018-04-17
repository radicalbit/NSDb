import { combineEpics } from 'redux-observable';
import { Observable } from 'rxjs';
import { types as TYPES } from './types';
import { actions } from './actions';
import { fetchHistoricalQuery, runRealtimeQuery } from '../../../api/nsdbService';

const fetchHistoricalQueryEpic = action$ =>
  action$.ofType(TYPES.HISTORICAL_QUERY_FETCH_REQUEST).switchMap(({ payload }) =>
    Observable.fromPromise(fetchHistoricalQuery(payload.query))
      .map(response => actions.fetchHistoricalQuerySuccess(payload.id, response))
      .catch(error =>
        Observable.of(actions.fetchHistoricalQueryError(error.xhr ? error.xhr.response : error))
      )
  );

const runRealtimeQueryEpic = action$ =>
  action$.ofType(TYPES.REALTIME_QUERY_START_SOCKET).mergeMap(({ payload }) => {
    const subject = runRealtimeQuery(payload.id);
    subject.next(JSON.stringify(payload.query));
    return subject
      .withLatestFrom(
        action$
          .ofType(TYPES.REALTIME_QUERY_STOP_SOCKET, TYPES.RESULT_REMOVE)
          .startWith(actions.stopRealtimeQuerySocket(null))
          .map(({ payload }) => payload.id)
      )
      .takeWhile(([msg, stopId]) => stopId !== payload.id)
      .map(([msg, _]) => actions.onRealtimeQueryMessage(payload.id, msg))
      .catch(error => Observable.of(actions.onRealtimeQueryError(error)));
  });

const queryEpic = combineEpics(fetchHistoricalQueryEpic, runRealtimeQueryEpic);

export default queryEpic;

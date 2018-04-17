import 'rxjs';
import { combineEpics } from 'redux-observable';
import { epic as nsdbEpic } from './nsdb';

export default function createEpic() {
  return combineEpics(nsdbEpic);
}

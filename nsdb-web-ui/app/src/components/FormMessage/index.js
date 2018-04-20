import React from 'react';

import './index.less';

const FormMessage = ({ touched, error, warning, success }) => (
  <div className="FormMessage">
    {touched && error ? (
      <span className="FormMessage-message FormMessage-error">{error}</span>
    ) : null}
    {touched && !error && warning ? (
      <span className="FormMessage-message FormMessage-warning">{warning}</span>
    ) : null}
    {touched && !error && !warning && success ? (
      <span className="FormMessage-message FormMessage-success">{success}</span>
    ) : null}
    {!error && !warning && !success && <span className="FormMessage-placeholder">placeholder</span>}
  </div>
);

export default FormMessage;

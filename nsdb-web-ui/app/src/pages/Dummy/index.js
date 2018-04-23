import React from 'react';
import { hot } from 'react-hot-loader';

import './index.less';

class Dummy extends React.Component {
  render() {
    console.log(this.props);
    return <div className="Dummy">Dummy page for testing routes</div>;
  }
}

export default hot(module)(Dummy);

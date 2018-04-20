import React from 'react';
import { Helmet } from 'react-helmet';
import AntdLayout from 'antd/lib/layout';

import './index.less';

const AntdHeader = AntdLayout.Header;
const AntdContent = AntdLayout.Content;
const AntdFooter = AntdLayout.Footer;

class Layout extends React.Component {
  render() {
    return (
      <div className="Layout">
        <Helmet titleTemplate="%s - NSDB Web UI" defaultTitle="NSDB Web UI">
          <meta name="description" content="Natural Series Database Web UI" />
        </Helmet>
        <AntdLayout>
          <AntdHeader className="Layout-header">
            <h1>NSDB</h1>
          </AntdHeader>
          <AntdContent className="Layout-content">{this.props.children}</AntdContent>
          <AntdFooter className="Layout-footer">
            <h3>Released under Apache 2 license</h3>
          </AntdFooter>
        </AntdLayout>
      </div>
    );
  }
}

export default Layout;

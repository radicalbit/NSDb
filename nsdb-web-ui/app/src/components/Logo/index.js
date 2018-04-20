import React from 'react';

import './index.less';

const Logo = ({ icon, image, alt, collapsed = false }) => (
  <div className="Logo">
    {collapsed ? (
      <img className="Logo-icon" src={icon} alt={alt} />
    ) : (
      <img className="Logo-image" src={image} alt={alt} />
    )}
  </div>
);

export default Logo;

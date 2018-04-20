import React from 'react';
import { Field } from 'react-form';
import Form from 'antd/lib/form';
import Input from 'antd/lib/input';

import FormMessage from '../../components/FormMessage';

import './index.less';

const FormItem = Form.Item;

const FormInputText = props => (
  <div className="FormInputText">
    <Field validate={props.validate} field={props.field}>
      {fieldApi => {
        const {
          label,
          field,
          validate,
          labelCol = { span: 24 },
          wrapperCol = { span: 24 },
          onChange,
          onBlur,
          ...rest
        } = props;
        const { value, touched, error, warning, success, setValue, setTouched } = fieldApi;

        return (
          <FormItem
            className="FormInputText-item"
            colon={false}
            label={label}
            labelCol={labelCol}
            wrapperCol={wrapperCol}
          >
            <Input
              {...rest}
              value={value || ''}
              onChange={e => {
                setValue(e.target.value);
                if (onChange) {
                  onChange(e.target.value, e);
                }
              }}
              onBlur={e => {
                setTouched();
                if (onBlur) {
                  onBlur(e);
                }
              }}
            />
            {validate ? (
              <FormMessage touched={touched} error={error} warning={warning} success={success} />
            ) : null}
          </FormItem>
        );
      }}
    </Field>
  </div>
);

export default FormInputText;

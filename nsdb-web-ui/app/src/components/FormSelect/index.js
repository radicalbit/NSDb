import React from 'react';
import { Field } from 'react-form';
import Form from 'antd/lib/form';
import Select from 'antd/lib/select';

import FormMessage from '../../components/FormMessage';

import './index.less';

const FormItem = Form.Item;
const Option = Select.Option;

const toSelectOptions = options =>
  options.map(
    option =>
      typeof option === 'object' ? (
        <Option key={String(option.value)} value={String(option.value)}>
          {option.label}
        </Option>
      ) : (
        <Option key={String(option)} value={String(option)}>
          {option}
        </Option>
      )
  );

const FormSelect = props => (
  <div className="FormSelect">
    <Field validate={props.validate} field={props.field}>
      {fieldApi => {
        const {
          options = [],
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
            className="FormSelect-item"
            colon={false}
            label={label}
            labelCol={labelCol}
            wrapperCol={wrapperCol}
          >
            <Select
              {...rest}
              value={value || ''}
              onChange={(value, option) => {
                setValue(value);
                if (onChange) {
                  onChange(value, option);
                }
              }}
              onBlur={option => {
                setTouched();
                if (onBlur) {
                  onBlur(option);
                }
              }}
            >
              {toSelectOptions(options)}
            </Select>
            {validate ? (
              <FormMessage touched={touched} error={error} warning={warning} success={success} />
            ) : null}
          </FormItem>
        );
      }}
    </Field>
  </div>
);

export default FormSelect;

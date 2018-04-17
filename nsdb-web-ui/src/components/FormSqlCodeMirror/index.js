import React from 'react';
import { Field } from 'react-form';
import { Controlled as CodeMirror } from 'react-codemirror2';

import FormMessage from '../../components/FormMessage';

import 'codemirror/mode/sql/sql';
import './index.less';

const FormSqlCodeMirror = props => (
  <div className="FormSqlCodeMirror">
    <Field validate={props.validate} field={props.field}>
      {fieldApi => {
        const { validate, onChange, onBlur } = props;
        const { value, touched, error, warning, success, setValue, setTouched } = fieldApi;

        return (
          <React.Fragment>
            <CodeMirror
              className="FormSqlCodeMirror-editor"
              value={value}
              options={{
                mode: 'text/x-sql',
                theme: 'material',
                lineNumbers: true,
              }}
              onBeforeChange={(editor, data, value) => {
                setValue(value);
                if (onChange) {
                  onChange(editor, data, value);
                }
              }}
              onChange={(editor, data, value) => {}}
              onBlur={(editor, e) => {
                setTouched();
                if (onBlur) {
                  onBlur(editor, e);
                }
              }}
            />
            {validate ? (
              <FormMessage touched={touched} error={error} warning={warning} success={success} />
            ) : null}
          </React.Fragment>
        );
      }}
    </Field>
  </div>
);

export default FormSqlCodeMirror;

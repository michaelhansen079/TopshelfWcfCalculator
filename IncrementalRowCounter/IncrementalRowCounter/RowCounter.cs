using System.Collections.Generic;
using System.Linq;
using Microsoft.SqlServer.Dts.Pipeline;
using Microsoft.SqlServer.Dts.Pipeline.Wrapper;
using Microsoft.SqlServer.Dts.Runtime.Wrapper;

namespace IncrementalRowCounter
{
    [DtsPipelineComponent(DisplayName = "IncrementalRowCounter"
                        , Description = "IncrementalRowCounter by mzhtech.com"
                        , IconResource = "IncrementalRowCounter.RowCounter.ico"
                        , ComponentType = ComponentType.Transform)]
    public class RowCounter : PipelineComponent
    {
        #region Constants

        private const string InputName = "Input";
        private const string OutputName = "Output";
        private const string IncrementalPropertyName = "Count rows incremental";
        private const string VariablePropertyName = "Variable";

        #endregion

        #region Variables

        private readonly IEnumerable<DataType> _allowedDataTypes = new List<DataType> { DataType.DT_I4, DataType.DT_I8, DataType.DT_UI4, DataType.DT_UI8 };

        private int _rowCount;
        private bool _rowCntAdded;

        #endregion

        #region DesignTime

        public override void ProvideComponentProperties()
        {
            // Set component information
            ComponentMetaData.Name = "IncrementalRowCounter";
            ComponentMetaData.Description = "Counts rows and and adds count to variable";
            ComponentMetaData.ContactInfo = "support@mydomain.com";

            // Reset the component input and outputs
            this.RemoveAllInputsOutputsAndCustomProperties();

            // Add custom properties
            AddCustomProperty(this.ComponentMetaData.CustomPropertyCollection, IncrementalPropertyName, "Whether variable is increased incremental or reset", true);
            AddCustomProperty(this.ComponentMetaData.CustomPropertyCollection, VariablePropertyName, "User variable to store row count", null);

            // Input
            IDTSInput100 input = ComponentMetaData.InputCollection.New();
            input.Name = InputName;

            // Output - ID of output = ID of input -> synchronous component
            IDTSOutput100 output = ComponentMetaData.OutputCollection.New();
            output.Name = OutputName;
            output.SynchronousInputID = input.ID;
            output.IsErrorOut = false;
        }

        private void AddCustomProperty(IDTSCustomPropertyCollection100 propertyCollection, string name, string description, object value)
        {
            IDTSCustomProperty100 property = propertyCollection.New();
            property.Name = name;
            property.Description = description;
            property.Value = value;
            property.ExpressionType = DTSCustomPropertyExpressionType.CPET_NONE;
            property.State = DTSPersistState.PS_DEFAULT;
        }

        public override void OnInputPathAttached(int inputID)
        {
            IDTSInput100 input = ComponentMetaData.InputCollection.GetObjectByID(inputID);

            IDTSVirtualInput100 vInput = input.GetVirtualInput();

            //Add all input columns
            foreach (IDTSVirtualInputColumn100 vCol in vInput.VirtualInputColumnCollection)
                SetUsageType(inputID, vInput, vCol.LineageID, DTSUsageType.UT_READONLY);
        }

        public override DTSValidationStatus Validate()
        {
            if (ComponentMetaData.InputCollection[0].InputColumnCollection.Count == 0)
            {
                PostError(string.Format("{0} not connected", InputName));
                return DTSValidationStatus.VS_ISBROKEN;
            }

            string variableName = (string)GetPropertyValue(ComponentMetaData.CustomPropertyCollection, VariablePropertyName);

            if (string.IsNullOrEmpty(variableName))
            {
                PostError(string.Format("{0} not set", VariablePropertyName));
                return DTSValidationStatus.VS_ISBROKEN;
            }

            if (!VariableDispenser.Contains(variableName))
            {
                PostError(string.Format("Variable {0} not found", variableName));
                return DTSValidationStatus.VS_ISBROKEN;
            }

            if (IsVariableSystemVariable(variableName))
            {
                PostError("Variable must be a UserVariable");
                return DTSValidationStatus.VS_ISBROKEN;
            }

            if (IsVariableReadOnly(variableName))
            {
                PostError("Variable is set to read only");
                return DTSValidationStatus.VS_ISBROKEN;
            }

            if (!_allowedDataTypes.Any(x => (int)x == GetVariableDataType(variableName)))
            {
                PostError(string.Format("Data type must be INT32, INT64, UINT32 or UINT64"));
                return DTSValidationStatus.VS_ISBROKEN;
            }

            return DTSValidationStatus.VS_ISVALID;
        }

        private object GetPropertyValue(IDTSCustomPropertyCollection100 propertyCollection, string name)
        {
            for (int i = 0; i < propertyCollection.Count; i++)
            {
                IDTSCustomProperty100 property = propertyCollection[i];
                if (property.Name.Equals(name))
                {
                    return property.Value;
                }
            }

            return null;
        }

        private object GetVariableValue(string variableName)
        {
            IDTSVariables100 variables = null;
            VariableDispenser.LockOneForRead(variableName, ref variables);
            var val = variables[0].Value;
            variables.Unlock();
            return val;
        }

        private bool IsVariableSystemVariable(string variableName)
        {
            IDTSVariables100 variables = null;
            VariableDispenser.LockOneForRead(variableName, ref variables);
            var isSystemVariable = variables[0].SystemVariable;
            variables.Unlock();
            return isSystemVariable;
        }

        private bool IsVariableReadOnly(string variableName)
        {
            IDTSVariables100 variables = null;
            VariableDispenser.LockOneForRead(variableName, ref variables);
            var isVariableReadonly = variables[0].ReadOnly;
            variables.Unlock();
            return isVariableReadonly;
        }

        private int GetVariableDataType(string variableName)
        {
            IDTSVariables100 variables = null;
            VariableDispenser.LockOneForRead(variableName, ref variables);
            var dataType = variables[0].DataType;
            variables.Unlock();
            return dataType;
        }

        private void SetVariableValue(string variableName, object val)
        {
            IDTSVariables100 variables = null;
            VariableDispenser.LockOneForWrite(variableName, ref variables);
            variables[0].Value = val;
            variables.Unlock();
        }

        private void PostError(string errorMessage)
        {
            bool cancelled;
            ComponentMetaData.FireError(1, ComponentMetaData.Name, errorMessage, string.Empty, 0, out cancelled);
        }

        public override void ReinitializeMetaData()
        {
            ComponentMetaData.RemoveInvalidInputColumns();
            base.ReinitializeMetaData();
        }

        #endregion

        #region Runtime

        public override void ProcessInput(int inputID, PipelineBuffer buffer)
        {
            if (!buffer.EndOfRowset)
            {
                while (buffer.NextRow())
                {
                    _rowCount++;
                }

            }
            else // Buffer processed
            {
                if (!_rowCntAdded)
                {
                    string variableName =
                        (string)GetPropertyValue(ComponentMetaData.CustomPropertyCollection, VariablePropertyName);
                    var isIncremental =
                        (bool)GetPropertyValue(ComponentMetaData.CustomPropertyCollection, IncrementalPropertyName);
                    if (isIncremental)
                    {
                        var variableRowCntVal = (int)GetVariableValue(variableName);
                        _rowCount += variableRowCntVal;
                    }

                    SetVariableValue(variableName, _rowCount);
                    _rowCntAdded = true;
                }
            }
        }

        #endregion

    }
}

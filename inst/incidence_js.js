<Script>
// Custom range filter with value label
      function rangeFilter(column, state) {
        // Get min and max values from raw table data
        let min = Infinity;
        let max = 0;
        state.data.forEach(function(row) {
          const value = row[column.id];
          if (value < min) {
            min = Math.floor(value);
          } else if (value > max) {
            max = Math.ceil(value);
          }
        });
      
        const filterValue = column.filterValue || min;
        const input = React.createElement('input', {
          type: 'range',
          value: filterValue,
          min: min,
          max: max,
          onChange: function(event) {
            // Set to undefined to clear the filter
            column.setFilter(event.target.value || undefined);
          },
          style: { width: '100%', marginRight: '8px' },
          'aria-label': 'Filter ' + column.name
        });
      
        return React.createElement(
          'div',
          { style: { display: 'flex', alignItems: 'center', height: '100%' } },
          [input, filterValue]
        );
      }
      
      // Filter method that filters numeric columns by minimum value
      function filterMinValue(rows, columnId, filterValue) {
        return rows.filter(function(row) {
          return row.values[columnId] >= filterValue;
        });
      }
<Script>
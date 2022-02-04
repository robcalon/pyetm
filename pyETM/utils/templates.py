import pandas

class Templates:
    
    def make_output_mapping_template(self, carriers=None):

        # add string to list
        if isinstance(carriers, str):
            carriers = [carriers]

        # carrier for which columns are fetched
        if carriers is None:
            carriers = ['electricity', 'heat', 'hydrogen', 'methane']

        if not isinstance(carriers, list):
            raise TypeError('carriers must be of type list')
        
        # regex mapping for product group
        productmap = {
            '^.*[.]output [(]MW[)]$': 'supply',
            '^.*[.]input [(]MW[)]$': 'demand',
            'deficit': 'supply',
        }

        def get_params(carrier):
            """helper for list comprehension"""

            # get curve columns
            curve = f'hourly_{carrier}_curves'
            idx = getattr(self, curve).columns        

            return pandas.Series(data=carrier, index=idx, dtype='str')

        # make output mapping
        mapping = [get_params(carrier) for carrier in carriers]
        mapping = pandas.concat(mapping).to_frame(name='carrier')

        # add product columns
        mapping['product'] = mapping.index.copy()
        mapping['product'] = mapping['product'].replace(productmap, regex=True)

        # set index name
        mapping.index.name = 'ETM_key'

        return mapping
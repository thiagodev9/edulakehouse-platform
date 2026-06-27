class SchemaValidator:

    @staticmethod
    def validate(df, expected_columns):

        print("\n" + "=" * 60)
        print("SCHEMA VALIDATION")
        print("=" * 60)

        current_columns = set(df.columns)

        expected_columns = set(expected_columns)

        missing_columns = expected_columns - current_columns

        new_columns = current_columns - expected_columns

        # ===============================
        # Missing columns (Destructive)
        # ===============================

        if missing_columns:

            print("\n❌ MUDANÇA DESTRUTIVA DETECTADA")

            for column in sorted(missing_columns):

                print(f"   - {column}")

            raise ValueError(
                "Pipeline interrompida. Schema incompatível."
            )

        # ===============================
        # New columns (Additive)
        # ===============================

        if new_columns:

            print("\n⚠ NOVAS COLUNAS DETECTADAS")

            for column in sorted(new_columns):

                print(f"   + {column}")

            print("\nMudança aditiva aceita.")

        else:

            print("\nNenhuma nova coluna encontrada.")

        print("\n✔ Schema validado com sucesso.")
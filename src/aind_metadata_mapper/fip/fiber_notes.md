# Fiber Photometry Implementation Notes

## Excitation-Emission Mappings
| Excitation | Wavelength (nm) | Emission Type |
|------------|----------------|---------------|
| Blue       | 470           | Green         |
| UV         | 415           | Isosbestic    |
| Yellow     | 560           | Red           |

## Camera Configuration
- Two cameras: green and red
- IR LED is used for behavior recording only (face illumination), not fiber photometry

## Fiber Numbering Conventions
- Fiber 0 is most anterior
- For matching anterior dimensions, left side gets smaller number
- File standard: Use "ROI0 (corresponding to fiber branch0) values" format

## Legacy Notes
### Patch Cord Naming (A-D)
Historical naming convention that should be deprecated. Origin unclear per Kenta.

## Future Considerations
1. Consider adding notes field to light sources configuration to clarify purpose
   - Example: Explicitly document that IR LED is for face illumination
2. Review and standardize ROI-to-fiber branch mapping documentation
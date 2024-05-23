Sub FormatExcelFunction()
	Sex = InputBox("M for Male" &vbCrlf &"F for Female")
	'150, 200, 255
	If Sex = "M" Or Sex = "m" Then
        RedCode = 155
        GreenCode = 200
        BlueCode = 255
    ElseIf Sex = "F" Or Sex = "f" Then
        RedCode = 255
        GreenCode = 220
        BlueCode = 255
    Else
        RedCode = 210
        GreenCode = 210
        BlueCode = 210
    End If
	
    rowAddress = Range("A1").End(xlDown).Offset(0, 0).Address

    firstRow = Range(rowAddress).Row
    
    colAddress = Range("A1").End(xlToRight).Offset(0, 0).Address
    
    firstCol = Range(colAddress).Column
    
    'MsgBox firstRow & " " & firstCol
    
    Range("A1").UnMerge
    
    lastRow = ActiveSheet.Cells.SpecialCells(xlCellTypeLastCell).Row

    'lastCol = ActiveSheet.Cells.SpecialCells(xlCellTypeLastCell).Column

    lastCol = ActiveSheet.Cells.Find("*", SearchOrder:=xlByColumns, SearchDirection:=xlPrevious).Column
    
    Range(Cells(1, 1), Cells(firstRow - 1, lastCol)).Select
    Selection.Font.Bold = True
    Selection.VerticalAlignment = xlCenter

    Cells.ColumnWidth = 8
    Range(Cells(firstRow, 1), Cells(lastRow, lastCol)).RowHeight = 12
    ActiveWindow.Zoom = 100
    Cells.Select
    With Selection.Font
        .Name = "Arial"
        .Size = 8
    End With
    
    Columns("A:A").ColumnWidth = 16
    Columns("B:B").ColumnWidth = 24
    Range("A1").UnMerge
    
    Columns("A:A").Font.Bold = True
    Columns("B:B").WrapText = False

    For i = firstRow To lastRow
        For u = 2 To (firstCol - 1)
            cellValue = Cells(i, u).Value
            If Len(cellValue) > 0 Then
                If cellValue = "Total" Then
                    Range(Cells(i, firstCol), Cells(i, lastCol)).Interior.Color = RGB(0, 0, 0)
                    Range(Cells(i, firstCol), Cells(i, lastCol)).Font.Bold = True
                    Cells(i, u).Font.Size = 9
                    Cells(i, u).Font.Bold = True
                    If u = 2 Then
                        Range(Cells(i, 1), Cells(i, lastCol)).Select
                        With Selection.Borders(xlEdgeTop)
                            .LineStyle = xlContinuous
                            .ColorIndex = xlAutomatic
                            .TintAndShade = 0
                            .Weight = xlMedium
                        End With
                    End If
                ElseIf cellValue = "DEL (R)" Then
                    Range(Cells(i, 2), Cells(i, lastCol)).EntireRow.Delete

                ElseIf cellValue = "Detractors" Or cellValue = "Passive" Or cellValue = "Passives" Or cellValue = "Promoters" Or cellValue = "Total" Or cellValue = "NPS" Or cellValue = "TB" Or cellValue = "T2B" Or cellValue = "T3B" Or cellValue = "Mean" Or cellValue = "Mean." Or cellValue = "Standard Deviation" Or cellValue = "B3B" Or cellValue = "B2B" Then
                    Range(Cells(i, firstCol), Cells(i, lastCol)).Font.Bold = True
                    Cells(i, u).Font.Bold = True
                'ElseIf cellValue = "Column N %" Or cellValue = "Count" Or cellValue = "Responses" Or cellValue = "Column Responses %" Then
                '    Cells(i, u).Value = ""
                End If

            End If
        Next
    Next

    lastRow = ActiveSheet.Cells.SpecialCells(xlCellTypeLastCell).Row

    For i = firstRow To lastRow
        For u = 2 To (firstCol - 1)
            cellValue = Cells(i, u).Value
            If Len(cellValue) > 0 Then
                If cellValue = "Total" Then
                    Range(Cells(i, firstCol), Cells(i, lastCol)).Interior.Color = RGB(RedCode, GreenCode, BlueCode)
                    Range(Cells(i, firstCol), Cells(i, lastCol)).Font.Bold = True
                    Cells(i, u).Font.Size = 9
                    Cells(i, u).Font.Bold = True
                End If
            End If
        Next
    Next


    Dim Rng As Range, y As Long
    For Each Rng In ActiveSheet.UsedRange.Cells
      For y = 1 To 7
        Rng.Errors(y).Ignore = True
      Next
    Next
    
    lastRow = ActiveSheet.Cells.SpecialCells(xlCellTypeLastCell).Row

    mergeRow = 0
    For p = 1 To 10
        If Cells(lastRow - p, lastCol).MergeCells Then
            Cells(lastRow - p, lastCol).UnMerge
            mergeRow = p
        End If
    Next

    Range(Cells(firstRow, firstCol), Cells(lastRow - mergeRow - 1, lastCol)).Select
    With Application.ReplaceFormat.Interior
        .PatternColorIndex = xlAutomatic
        .Color = 10086143
        .TintAndShade = 0
        .PatternTintAndShade = 0
    End With

    Application.DisplayAlerts = False

    Selection.Replace What:="A", Replacement:="A", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=True, SearchFormat:=False, ReplaceFormat:=True
    Selection.Replace What:="B", Replacement:="B", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=True, SearchFormat:=False, ReplaceFormat:=True
    Selection.Replace What:="C", Replacement:="C", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=True, SearchFormat:=False, ReplaceFormat:=True
    Selection.Replace What:="D", Replacement:="D", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=True, SearchFormat:=False, ReplaceFormat:=True
    Selection.Replace What:="E", Replacement:="E", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=True, SearchFormat:=False, ReplaceFormat:=True
    Selection.Replace What:="F", Replacement:="F", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=True, SearchFormat:=False, ReplaceFormat:=True
    Selection.Replace What:="G", Replacement:="G", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=True, SearchFormat:=False, ReplaceFormat:=True
    Selection.Replace What:="H", Replacement:="H", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=True, SearchFormat:=False, ReplaceFormat:=True
    Selection.Replace What:="I", Replacement:="I", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=True, SearchFormat:=False, ReplaceFormat:=True
    Selection.Replace What:="J", Replacement:="J", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=True, SearchFormat:=False, ReplaceFormat:=True
    Selection.Replace What:="K", Replacement:="K", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=True, SearchFormat:=False, ReplaceFormat:=True
    Selection.Replace What:="L", Replacement:="L", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=True, SearchFormat:=False, ReplaceFormat:=True
    Selection.Replace What:="M", Replacement:="M", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=True, SearchFormat:=False, ReplaceFormat:=True
    Selection.Replace What:="N", Replacement:="N", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=True, SearchFormat:=False, ReplaceFormat:=True
    Selection.Replace What:="O", Replacement:="O", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=True, SearchFormat:=False, ReplaceFormat:=True
    Selection.Replace What:="P", Replacement:="P", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=True, SearchFormat:=False, ReplaceFormat:=True
    Selection.Replace What:="Q", Replacement:="Q", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=True, SearchFormat:=False, ReplaceFormat:=True
    Selection.Replace What:="R", Replacement:="R", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=True, SearchFormat:=False, ReplaceFormat:=True
    Selection.Replace What:="S", Replacement:="S", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=True, SearchFormat:=False, ReplaceFormat:=True
    Selection.Replace What:="T", Replacement:="T", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=True, SearchFormat:=False, ReplaceFormat:=True
    Selection.Replace What:="U", Replacement:="U", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=True, SearchFormat:=False, ReplaceFormat:=True
    Selection.Replace What:="V", Replacement:="V", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=True, SearchFormat:=False, ReplaceFormat:=True
    Selection.Replace What:="W", Replacement:="W", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=True, SearchFormat:=False, ReplaceFormat:=True
    Selection.Replace What:="X", Replacement:="X", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=True, SearchFormat:=False, ReplaceFormat:=True
    Selection.Replace What:="Y", Replacement:="Y", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=True, SearchFormat:=False, ReplaceFormat:=True
    Selection.Replace What:="Z", Replacement:="Z", LookAt:=xlPart, _
        SearchOrder:=xlByRows, MatchCase:=True, SearchFormat:=False, ReplaceFormat:=True

    Application.DisplayAlerts = True
    
    For r = 1 To mergeRow
        Range(Cells(lastRow - r, 1), Cells(lastRow - r, lastCol)).Merge
    Next

    Range(Cells(1, 1), Cells(firstRow - 1, firstCol - 1)).Merge

    'If WorksheetFunction.CountA(Columns(firstCol - 1)) = 0 Then
    '    Columns(firstCol - 1).Delete
    'End If

    Range("A1") = "DATA TABLE "
    Range("A1").Select
    With Selection.Font
        .Name = "Arial"
        .Size = 18
    End With
    Selection.VerticalAlignment = xlCenter
    Selection.HorizontalAlignment = xlCenter
    Cells(1, 1).Select
End Sub